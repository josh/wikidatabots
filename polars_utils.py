# pyright: strict

import os
import random
import re
import sys
import xml.etree.ElementTree as ET
from functools import partial, reduce
from itertools import combinations
from math import ceil
from typing import Any, Callable, Iterable, Iterator, TypeVar

import polars as pl
from tqdm import tqdm


def all_exprs(exprs: Iterable[pl.Expr]) -> pl.Expr:
    return reduce(pl.Expr.__and__, exprs)


def any_exprs(exprs: Iterable[pl.Expr]) -> pl.Expr:
    return reduce(pl.Expr.__or__, exprs)


def update_parquet(
    filename: str,
    transform: Callable[[pl.LazyFrame], pl.LazyFrame],
) -> None:
    assert filename.endswith(".parquet")
    df = pl.scan_parquet(filename)
    df2 = transform(df)
    assert df2.schema == df.schema, "schema changed"
    tmpfile = f"{filename}.{random.randint(0, 2**32)}"
    df2.collect().write_parquet(
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
    ).join(df, on=name, how="left", allow_parallel=False)


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

    other = other.join(
        df.drop(other_cols), on=on, how="left", allow_parallel=False
    ).select(df.columns)
    return pl.concat(
        [df, other],
        parallel=False,  # BUG: parallel caching is broken
    ).unique(subset=on, keep="last", maintain_order=True)


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
        left_df.join(right_df, on=on, how="outer", suffix=suffix, allow_parallel=False)
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
    any_updated_col = reduce(pl.Expr.__or__, updated_cols)

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
    def apply_function(s: pl.Series) -> Iterator[Any]:
        if len(s) == 0:
            return

        try:
            print(f"::group::{log_group}", file=sys.stderr)
            for item in tqdm(s, unit="row"):
                if item:
                    yield function(item)
                else:
                    yield None
        finally:
            print("::endgroup::", file=sys.stderr)

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


def xml_to_dtype(
    xml: str,
    dtype: pl.List,
    xpath: str = "./*",
) -> list[XMLValue]:
    inner_dtype = dtype.inner
    assert isinstance(inner_dtype, pl.Struct)
    root = ET.fromstring(xml)
    return [_xml_element_struct_field(el, inner_dtype) for el in root.findall(xpath)]


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
            expr = reduce(pl.Expr.__and__, exprs)
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
