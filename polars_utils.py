# pyright: strict

import os
import random
import re
import sys
import xml.etree.ElementTree as ET
from functools import partial
from itertools import combinations
from math import ceil
from typing import Any, Callable, Iterable, Iterator, TextIO, TypeVar

import polars as pl
from tqdm import tqdm

from actions import log_group as _log_group
from actions import warn


def github_step_summary() -> TextIO:
    if "GITHUB_STEP_SUMMARY" in os.environ:
        return open(os.environ["GITHUB_STEP_SUMMARY"], "a")
    else:
        return sys.stderr


def update_parquet(
    filename: str,
    transform: Callable[[pl.LazyFrame], pl.LazyFrame],
    key: str,
) -> None:
    assert filename.endswith(".parquet")
    df = pl.read_parquet(filename)

    lf2 = transform(df.lazy())
    assert lf2.schema == df.schema, "schema changed"
    df2 = lf2.collect()

    describe_frame_with_diff(
        df,
        df2,
        key=key,
        source=filename,
        output=github_step_summary(),
    )

    tmpfile = f"{filename}.{random.randint(0, 2**32)}"
    df2.write_parquet(
        tmpfile,
        compression="zstd",
        statistics=True,
    )
    os.rename(tmpfile, filename)


def _check_ldf(
    ldf: pl.LazyFrame,
    function: Callable[[pl.DataFrame], None],
) -> pl.LazyFrame:
    def _inner_check(df: pl.DataFrame) -> pl.DataFrame:
        function(df)
        return df

    return ldf.map(_inner_check)


def assert_expression(
    ldf: pl.LazyFrame, expr: pl.Expr, message: str = ""
) -> pl.LazyFrame:
    def assert_expression_inner(df: pl.DataFrame) -> None:
        for name, series in df.select(expr).to_dict().items():
            assert series.dtype == pl.Boolean
            assert series.is_empty() or series.all(), message.format(name)

    return _check_ldf(ldf, assert_expression_inner)


def expr_repl(expr: pl.Expr, strip_alias: bool = False) -> str:
    expr_s: str = str(expr)
    if strip_alias:
        expr_s = re.sub(r'\.alias\("\w+"\)', "", expr_s)
    return f"pl.{expr_s}"


def filter_columns(df: pl.DataFrame, predicate: pl.Expr) -> pl.DataFrame:
    return df.select(_flagged_columns(df, predicate))


def drop_columns(df: pl.DataFrame, predicate: pl.Expr) -> pl.DataFrame:
    return df.drop(_flagged_columns(df, predicate))


def _flagged_columns(df: pl.DataFrame, predicate: pl.Expr) -> list[str]:
    df = df.select(predicate)
    if df.is_empty():
        return []
    assert set(df.dtypes) == {pl.Boolean}, "predicate must return a boolean"
    assert len(df) == 1, "predicate must return a single row"
    row = df.row(0, named=True)
    return [col for col, flag in row.items() if flag]


def limit(
    df: pl.LazyFrame,
    n: tuple[int, int] = (sys.maxsize, sys.maxsize),
    soft: int = sys.maxsize,
    hard: int = sys.maxsize,
    desc: str = "frame",
) -> pl.LazyFrame:
    soft = min(soft, n[0])
    hard = min(hard, n[1])
    soft = min(soft, hard)

    def _limit(df: pl.DataFrame) -> pl.DataFrame:
        total = len(df)
        if total > hard:
            raise AssertionError(f"{desc} exceeded hard limit: {total:,}/{hard:,}")
        elif total > soft:
            warn(f"{desc} exceeded soft limit: {total:,}/{soft:,}")
            return df.sample(soft)
        else:
            return df

    return df.map(_limit)


def is_constant(expr: pl.Expr) -> pl.Expr:
    return (expr == expr.first()).all()


def groups_of(expr: pl.Expr, n: int) -> pl.Expr:
    assert n > 0
    groups_count = (pl.count() / n).ceil()

    return (
        expr.extend_constant(None, n)
        .head(groups_count * n)
        .reshape((-1, n))
        .arr.eval(pl.element().drop_nulls())
    )


def series_indicies(a: pl.Series, b: pl.Series) -> pl.Series:
    return (
        b.arg_sort()
        .take(  # type: ignore
            b.sort()
            .search_sorted(a, side="left")  # type: ignore
            .clip_max(b.len() - 1),
        )
        .zip_with(a.is_in(b), pl.Series([None], dtype=pl.UInt32))
    )


def series_indicies_sorted(a: pl.Series, b: pl.Series) -> pl.Series:
    return b.search_sorted(a, side="left").zip_with(  # type: ignore
        a.is_in(b), pl.Series([None], dtype=pl.UInt32)
    )


def expr_indicies_sorted(a: pl.Expr, b: pl.Expr) -> pl.Expr:
    return pl.when(a.is_in(b)).then(b.search_sorted(a, side="left")).otherwise(None)


PL_INTEGERS = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
}


def align_to_index(df: pl.LazyFrame, name: str) -> pl.LazyFrame:
    assert df.schema[name] in PL_INTEGERS

    df = df.pipe(
        assert_expression,
        pl.col(name).is_not_null() & pl.col(name).is_unique() & (pl.col(name) >= 0),
        f"Invalid {name} index",
    ).cache()

    return df.select(
        pl.arange(
            0,
            pl.coalesce([pl.col(name).max().cast(pl.Int64) + 1, 0]),
            dtype=df.schema[name],
        ).alias(name)
    ).join(df, on=name, how="left")


def update_or_append(df: pl.LazyFrame, other: pl.LazyFrame, on: str) -> pl.LazyFrame:
    df = (
        df.pipe(
            assert_expression,
            pl.col(on).is_not_null(),
            f"df '{on}' column has null values",
        )
        .pipe(
            assert_expression,
            pl.col(on).is_unique(),
            f"df '{on}' column has non-unique values",
        )
        .cache()
    )
    other = (
        other.pipe(
            assert_expression,
            pl.col(on).is_not_null(),
            f"other df '{on}' column has null values",
        )
        .pipe(
            assert_expression,
            pl.col(on).is_unique(),
            f"other df '{on}' column has non-unique values",
        )
        .cache()
    )

    other_cols = list(other.columns)
    other_cols.remove(on)

    other = other.join(df.drop(other_cols), on=on, how="left").select(df.columns)
    return pl.concat([df, other], parallel=False).unique(
        subset=on, keep="last", maintain_order=True
    )


_INDICATOR_EXPR = (
    pl.when(pl.col("_merge_left") & pl.col("_merge_right"))
    .then(pl.lit("both", dtype=pl.Categorical))
    .when(pl.col("_merge_left"))
    .then(pl.lit("left_only", dtype=pl.Categorical))
    .when(pl.col("_merge_right"))
    .then(pl.lit("right_only", dtype=pl.Categorical))
    .otherwise(None)
    .alias("_merge")
)


def merge_with_indicator(
    left_df: pl.LazyFrame,
    right_df: pl.LazyFrame,
    on: str | pl.Expr,
    suffix: str = "_right",
) -> pl.LazyFrame:
    left_df = left_df.with_columns(pl.lit(True).alias("_merge_left"))
    right_df = right_df.with_columns(pl.lit(True).alias("_merge_right"))
    return (
        left_df.join(right_df, on=on, how="outer", suffix=suffix)
        .with_columns(_INDICATOR_EXPR)
        .drop("_merge_left", "_merge_right")
    )


def frame_diff(
    df1: pl.LazyFrame,
    df2: pl.LazyFrame,
    on: str,
    suffix: str = "_updated",
) -> pl.LazyFrame:
    cols = [col for col in df1.columns if col != on]
    assert len(cols) >= 1

    compute_col_updated_exprs = [
        (
            pl.col(col)
            .pipe(pl.Expr.__ne__, pl.col(f"{col}_right"))
            .pipe(pl.Expr.__and__, pl.col("_merge") == "both")
            .alias(f"{col}{suffix}")
        )
        for col in cols
    ]

    updated_cols = [pl.col(f"{col}{suffix}") for col in cols]
    any_updated_col = pl.Expr.or_(*updated_cols)

    return (
        df1.pipe(merge_with_indicator, df2, on=on)
        .with_columns(compute_col_updated_exprs)
        .select(
            (pl.col("_merge") == "right_only").alias("added"),
            (pl.col("_merge") == "left_only").alias("removed"),
            any_updated_col.alias("updated"),
            *updated_cols,
        )
        .select(pl.all().sum())
    )


def apply_with_tqdm(
    expr: pl.Expr,
    function: Callable[[Any], Any],
    return_dtype: pl.PolarsDataType | None = None,
    log_group: str = "apply(unknown)",
) -> pl.Expr:
    def apply_function(s: pl.Series) -> list[Any]:
        values: list[Any] = []

        if len(s) == 0:
            return values

        with _log_group(log_group):
            for item in tqdm(s, unit="row"):
                if item:
                    values.append(function(item))
                else:
                    values.append(None)

        return values

    def map_function(s: pl.Series) -> pl.Series:
        return pl.Series(values=apply_function(s), dtype=return_dtype)

    return expr.map(map_function, return_dtype=return_dtype)


def read_xml(
    xml: str,
    schema: dict[str, pl.PolarsDataType],
    xpath: str = "./*",
) -> pl.DataFrame:
    tree = ET.fromstring(xml)
    dtype = pl.Struct([pl.Field(k, schema[k]) for k in schema])
    rows = [_xml_element_struct_field(row, dtype) for row in tree.findall(xpath)]
    return pl.from_dicts(rows, schema=schema)


def _parse_xml_to_series(
    xml: str,
    dtype: pl.Struct,
    xpath: str = "./*",
) -> pl.Series:
    tree = ET.fromstring(xml)
    rows = tree.findall(xpath)
    values = [_xml_element_struct_field(row, dtype) for row in rows]
    return pl.Series(values=values, dtype=dtype)


def xml_extract(
    expr: pl.Expr,
    dtype: pl.List,
    xpath: str = "./*",
    log_group: str = "apply(xml_extract)",
) -> pl.Expr:
    inner_dtype = dtype.inner
    assert isinstance(inner_dtype, pl.Struct)
    return apply_with_tqdm(
        expr,
        partial(_parse_xml_to_series, xpath=xpath, dtype=inner_dtype),
        return_dtype=dtype,
        log_group=log_group,
    )


XMLValue = dict[str, "XMLValue"] | list["XMLValue"] | str | int | float | None


def _xml_element_struct_field(
    element: ET.Element,
    dtype: pl.Struct,
) -> dict[str, XMLValue]:
    obj: dict[str, XMLValue] = {}
    for field in dtype.fields:
        if isinstance(field.dtype, pl.List):
            inner_dtype = field.dtype.inner
            assert inner_dtype
            values = _xml_element_field_iter(element, field.name, inner_dtype)
            obj[field.name] = list(values)
        else:
            values = _xml_element_field_iter(element, field.name, field.dtype)
            obj[field.name] = next(values, None)
    return obj


def _xml_element_field_iter(
    element: ET.Element,
    name: str,
    dtype: pl.PolarsDataType,
) -> Iterator[dict[str, XMLValue] | str | int | float]:
    assert not isinstance(dtype, pl.List)

    if name in element.attrib:
        yield element.attrib[name]

    for child in element:
        # strip xml namespace
        tag = child.tag.split("}")[-1]

        if tag == name:
            if isinstance(dtype, pl.Struct):
                yield _xml_element_struct_field(child, dtype)
            elif child.text and child.text.strip():
                if dtype is pl.Int64:
                    yield int(child.text)
                elif dtype is pl.Float64:
                    yield float(child.text)
                else:
                    yield child.text


T = TypeVar("T")


def assert_called_once() -> Callable[[T], T]:
    calls: int = 1

    def mock(value: T) -> T:
        nonlocal calls
        calls -= 1
        assert calls >= 0, "mock called too many times"
        return value

    return mock


_POWERSET_COL_SEP = "--7C69799--"


def outlier_exprs(
    df: pl.DataFrame,
    exprs: Iterable[pl.Expr],
    rmax: int = 10,
    max_count: int | None = None,
) -> list[tuple[str, pl.Expr, int]]:
    results: list[tuple[str, pl.Expr, int]] = []

    if not max_count:
        max_count = ceil(len(df) * 0.003)

    col_expr: dict[str, pl.Expr] = {}
    for expr in _expand_expr(df, exprs):
        col_expr[expr.meta.output_name()] = expr

    is_const_expr = pl.all().pipe(is_constant)

    df = df.select(col_expr.values()).pipe(drop_columns, is_const_expr)
    orig_columns = df.columns

    for r in range(2, min(len(orig_columns), rmax + 1)):
        col_exprs: list[pl.Expr] = []

        for cols in combinations(orig_columns, r):
            col1 = _POWERSET_COL_SEP.join(cols[:-1])
            col2 = cols[-1]
            if (col1 in df.columns) and (col2 in df.columns):
                col_exprs.append(
                    (pl.col(col1) & pl.col(col2)).alias(_POWERSET_COL_SEP.join(cols))
                )

        if col_exprs:
            df = df.with_columns(col_exprs).pipe(drop_columns, is_const_expr)

    df_total = df.select(pl.all().sum())

    if not len(df_total):
        return results

    row = df_total.row(0, named=True)

    for col, count in sorted(row.items(), key=lambda a: a[1]):
        if count > 0 and count < max_count:
            exprs = [col_expr[col] for col in col.split(_POWERSET_COL_SEP)]
            expr = pl.Expr.and_(*exprs)
            expr_str = " & ".join(expr_repl(expr, strip_alias=True) for expr in exprs)
            results.append((expr_str, expr, count))

    return results


def _expand_expr(df: pl.DataFrame, exprs: Iterable[pl.Expr]) -> Iterator[pl.Expr]:
    for expr, s in zip(exprs, df.select(exprs), strict=True):
        output_name = expr.meta.output_name()
        if s.is_boolean() and (s.all() ^ s.any()):
            yield expr
            yield expr.is_not().alias(f"not_{output_name}")

        null_count = s.null_count()
        if null_count > 0 and null_count < s.len():
            yield expr.is_null().alias(f"null_{output_name}")
            yield expr.is_not_null().alias(f"not_null_{output_name}")

        if not s.is_boolean():
            yield expr.is_duplicated().alias(f"duplicated_{output_name}")
            yield expr.is_unique().alias(f"unique_{output_name}")


def _format_int_comma(expr: pl.Expr) -> pl.Expr:
    return expr.apply(
        lambda v: f"{v:,}",
        return_dtype=pl.Utf8,
    )


def _format_float_percent(expr: pl.Expr) -> pl.Expr:
    return expr.apply(
        lambda v: f"{v:.2%}",
        return_dtype=pl.Utf8,
    )


def _dtype_str_repr(dtype: pl.PolarsDataType) -> str:
    if isinstance(dtype, pl.DataType):
        return dtype._string_repr()  # type: ignore
    else:
        return dtype._string_repr(dtype)  # type: ignore


def compute_stats(
    df: pl.DataFrame,
    changes_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    def _count_columns(column_name: str, expr: pl.Expr) -> pl.DataFrame:
        df2 = df.select(expr)
        if df2.is_empty():
            schema = {"column": pl.Utf8, column_name: pl.UInt32}
            return pl.DataFrame(schema=schema)
        return df2.transpose(include_header=True, column_names=[column_name])

    count = len(df)
    null_count_df = _count_columns("null_count", pl.all().null_count())
    is_unique_df = _count_columns("is_unique", pl.all().drop_nulls().is_unique().all())
    true_count_df = _count_columns("true_count", pl.col(pl.Boolean).drop_nulls().sum())
    false_count_df = _count_columns(
        "false_count", pl.col(pl.Boolean).drop_nulls().is_not().sum()
    )

    def _percent_col(name: str) -> pl.Expr:
        return (
            pl.when(pl.col(f"{name}_count") > 0)
            .then(
                pl.format(
                    "{} ({})",
                    pl.col(f"{name}_count").pipe(_format_int_comma),
                    (pl.col(f"{name}_count") / count).pipe(_format_float_percent),
                )
            )
            .otherwise(None)
            .alias(name)
        )

    def _int_col(name: str) -> pl.Expr:
        return (
            pl.when(pl.col(f"{name}_count") > 0)
            .then(pl.col(f"{name}_count").pipe(_format_int_comma))
            .otherwise(None)
            .alias(name)
        )

    joined_df = (
        null_count_df.join(is_unique_df, on="column", how="left")
        .join(true_count_df, on="column", how="left")
        .join(false_count_df, on="column", how="left")
    )

    if changes_df is not None:
        updated_count_df = changes_df.select(
            pl.col("^.+_updated$").map_alias(lambda n: n.replace("_updated", ""))
        ).transpose(include_header=True, column_names=["updated_count"])
        joined_df = joined_df.join(updated_count_df, on="column", how="left")
    else:
        joined_df = joined_df.with_columns(pl.lit(0).alias("updated_count"))

    return joined_df.select(
        pl.col("column").alias("name"),
        pl.col("column").apply(df.schema.get).apply(_dtype_str_repr).alias("dtype"),
        _percent_col("null"),
        _percent_col("true"),
        _percent_col("false"),
        pl.when(pl.col("is_unique")).then("true").otherwise("").alias("unique"),
        _int_col("updated"),
    ).fill_null("")


def describe_frame(
    df: pl.DataFrame,
    source: str,
    output: TextIO,
    changes_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    with pl.Config() as cfg:
        cfg.set_fmt_str_lengths(100)
        cfg.set_tbl_cols(-1)
        cfg.set_tbl_column_data_type_inline(True)
        cfg.set_tbl_formatting("ASCII_MARKDOWN")
        cfg.set_tbl_hide_dataframe_shape(True)
        cfg.set_tbl_rows(-1)
        cfg.set_tbl_width_chars(500)

        print(f"## {source}", file=output)
        print(compute_stats(df, changes_df=changes_df), file=output)
        print(f"\nshape: ({df.shape[0]:,}, {df.shape[1]:,})", file=output)

        if changes_df is not None:
            changes = changes_df.row(0, named=True)
            added, removed, updated = (
                changes["added"],
                changes["removed"],
                changes["updated"],
            )
            print(f"changes: +{added:,} -{removed:,} ~{updated:,}", file=output)

        mb = df.estimated_size("mb")
        if mb > 2:
            print(f"rss: {mb:,.1f}MB", file=output)
        else:
            kb = df.estimated_size("kb")
            print(f"rss: {kb:,.1f}KB", file=output)

    return df


def describe_frame_with_diff(
    df_old: pl.DataFrame,
    df_new: pl.DataFrame,
    key: str,
    source: str,
    output: TextIO,
) -> pl.DataFrame:
    changes_df = frame_diff(df_old.lazy(), df_new.lazy(), on=key).collect()
    return describe_frame(
        df_new,
        changes_df=changes_df,
        source=source,
        output=output,
    )


def describe_lazy(df: pl.LazyFrame, source: str, output: TextIO) -> pl.LazyFrame:
    return df.map(partial(describe_frame, source=source, output=output))
