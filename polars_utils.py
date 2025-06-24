import atexit
import datetime
import gzip
import html
import os
import random
import sys
import xml.etree.ElementTree as ET
import zlib
from collections.abc import Callable, Iterator
from functools import partial
from pathlib import Path
from typing import Any, TextIO, TypedDict, TypeVar

import numpy as np
import polars as pl
import polars.selectors as cs
from polars._typing import PolarsDataType
from tqdm import tqdm

from actions import log_group as _log_group
from actions import warn

SomeFrame = TypeVar("SomeFrame", pl.DataFrame, pl.LazyFrame)


def map_batches[SomeFrame: (pl.DataFrame, pl.LazyFrame)](
    df: SomeFrame, function: Callable[[pl.DataFrame], pl.DataFrame]
) -> SomeFrame:
    if isinstance(df, pl.LazyFrame):
        return df.map_batches(function)
    else:
        return function(df)


def github_step_summary() -> TextIO:
    if "GITHUB_STEP_SUMMARY" in os.environ:
        return open(os.environ["GITHUB_STEP_SUMMARY"], "a")
    else:
        return sys.stderr


class _PyformatRow(TypedDict):
    args: list[Any]
    kwargs: dict[str, Any]


def apply_with_tqdm(
    expr: pl.Expr,
    function: Callable[[Any], Any],
    return_dtype: PolarsDataType | None = None,
    log_group: str = "apply(unknown)",
) -> pl.Expr:
    def apply_function(s: pl.Series) -> list[Any]:
        values: list[Any] = []
        size = len(s)

        if size == 0:
            return values

        with _log_group(log_group):
            for item in tqdm(s, unit="row"):
                if item is None:
                    values.append(None)
                else:
                    values.append(function(item))

        return values

    def map_function(s: pl.Series) -> pl.Series:
        return pl.Series(values=apply_function(s), dtype=return_dtype)

    # MARK: pl.Expr.map_batches
    return expr.map_batches(map_function, return_dtype=return_dtype)


def lazy_map_reduce_batches(
    df: pl.LazyFrame,
    map_function: Callable[[pl.LazyFrame], pl.LazyFrame],
    reduce_function: Callable[[pl.DataFrame, pl.DataFrame], pl.DataFrame],
) -> pl.LazyFrame:
    def inner(df: pl.DataFrame) -> pl.DataFrame:
        # MARK: pl.DataFrame.lazy
        # MARK: pl.LazyFrame.collect
        df2 = map_function(df.lazy()).collect()
        return reduce_function(df, df2)

    return df.map_batches(inner)


def map_streaming(
    ldf: pl.LazyFrame,
    expr: pl.Expr,
    return_schema: dict[str, PolarsDataType],
    chunk_size: int,
    parallel: bool = False,
) -> pl.LazyFrame:
    assert chunk_size > 0

    def map_func(df: pl.DataFrame) -> pl.DataFrame:
        size = len(df)
        if size == 0:
            return df
        ldf = df.lazy()
        ldfs = [
            ldf.slice(offset, chunk_size).select(expr)
            for offset in range(0, size, chunk_size)
        ]
        return pl.concat(ldfs, how="vertical", parallel=parallel).collect()

    return ldf.map_batches(map_func, schema=return_schema)


def pyformat(
    format_string: str,
    *args: pl.Expr | str,
    **kwargs: pl.Expr | str,
) -> pl.Expr:
    def _format(row: _PyformatRow) -> str | None:
        row_args = row.get("args", [])
        row_kwargs = row.get("kwargs", {})

        if any(v is None for v in row_args):
            return None
        elif any(v is None for v in row_kwargs.values()):
            return None
        else:
            return format_string.format(*row_args, **row_kwargs)

    packed_expr: pl.Expr
    if len(args) > 0 and len(kwargs) > 0:
        packed_expr = pl.struct(
            args=pl.concat_list(*args),
            kwargs=pl.struct(**kwargs),  # type: ignore
        )
    elif len(args) > 0 and len(kwargs) == 0:
        packed_expr = pl.struct(args=pl.concat_list(*args))
    elif len(args) == 0 and len(kwargs) > 0:
        packed_expr = pl.struct(kwargs=pl.struct(**kwargs))  # type: ignore
    else:
        raise ValueError("must provide at least one argument")

    return packed_expr.pipe(
        apply_with_tqdm,
        _format,
        return_dtype=pl.Utf8,
        log_group="pyformat",
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
        left_df.join(right_df, on=on, how="full", coalesce=True, suffix=suffix)
        .with_columns(_INDICATOR_EXPR)
        .drop("_merge_left", "_merge_right")
    )


def frame_diff(
    df1: pl.LazyFrame,
    df2: pl.LazyFrame,
    on: str,
    suffix: str = "_updated",
) -> pl.LazyFrame:
    cols = [col for col in df1.collect_schema().names() if col != on]
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


def _dtype_str_repr(dtype: PolarsDataType) -> str:
    if isinstance(dtype, pl.DataType):
        return dtype._string_repr()
    else:
        return dtype._string_repr(dtype)  # type: ignore


def compute_raw_stats(df: pl.DataFrame) -> pl.DataFrame:
    def _count_columns(column_name: str, expr: pl.Expr) -> pl.DataFrame:
        df2 = df.select(expr)
        if df2.is_empty():
            schema = {"column": pl.Utf8, column_name: pl.UInt32}
            return pl.DataFrame(schema=schema)
        return df2.transpose(include_header=True, column_names=[column_name])

    null_count_df = _count_columns("null_count", pl.all().null_count())
    is_unique_df = _count_columns(
        "is_unique",
        _COL_SUPPORTS_UNIQUE.drop_nulls().is_unique().all(),
    )
    true_count_df = _count_columns("true_count", pl.col(pl.Boolean).drop_nulls().sum())
    false_count_df = _count_columns(
        "false_count", pl.col(pl.Boolean).drop_nulls().not_().sum()
    )

    joined_df = (
        null_count_df.join(is_unique_df, on="column", how="left", coalesce=True)
        .join(true_count_df, on="column", how="left", coalesce=True)
        .join(false_count_df, on="column", how="left", coalesce=True)
    )

    return joined_df.select(
        pl.col("column").alias("name"),
        # MARK: pl.Expr.map_elements
        pl.col("column")
        .map_elements(df.schema.get, return_dtype=pl.Object)
        .map_elements(_dtype_str_repr, return_dtype=pl.String)
        .alias("dtype"),
        pl.col("null_count"),
        pl.col("true_count"),
        pl.col("false_count"),
        pl.col("is_unique"),
    )


_COL_SUPPORTS_UNIQUE = (
    cs.binary() | cs.boolean() | cs.numeric() | cs.string() | cs.temporal()
)


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
    is_unique_df = _count_columns(
        "is_unique",
        _COL_SUPPORTS_UNIQUE.drop_nulls().is_unique().all(),
    )
    true_count_df = _count_columns("true_count", pl.col(pl.Boolean).drop_nulls().sum())
    false_count_df = _count_columns(
        "false_count", pl.col(pl.Boolean).drop_nulls().not_().sum()
    )

    def _percent_col(name: str) -> pl.Expr:
        return (
            pl.when(pl.col(f"{name}_count") > 0)
            .then(
                pyformat(
                    "{:,.0f} ({:.1%})",
                    pl.col(f"{name}_count"),
                    pl.col(f"{name}_count") / count,
                )
            )
            .otherwise(None)
            .alias(name)
        )

    def _int_col(name: str) -> pl.Expr:
        return (
            pl.when(pl.col(f"{name}_count") > 0)
            .then(pyformat("{:,}", pl.col(f"{name}_count")))
            .otherwise(None)
            .alias(name)
        )

    joined_df = (
        null_count_df.join(is_unique_df, on="column", how="left", coalesce=True)
        .join(true_count_df, on="column", how="left", coalesce=True)
        .join(false_count_df, on="column", how="left", coalesce=True)
    )

    if changes_df is not None:
        updated_count_df = changes_df.select(
            pl.col("^.+_updated$").name.map(lambda n: n.replace("_updated", ""))
        ).transpose(include_header=True, column_names=["updated_count"])
        joined_df = joined_df.join(
            updated_count_df, on="column", how="left", coalesce=True
        )
    else:
        joined_df = joined_df.with_columns(pl.lit(0).alias("updated_count"))

    return joined_df.select(
        pl.col("column").alias("name"),
        # MARK: pl.Expr.map_elements
        pl.col("column")
        .map_elements(df.schema.get, return_dtype=pl.Object)
        .map_elements(_dtype_str_repr, return_dtype=pl.String)
        .alias("dtype"),
        _percent_col("null"),
        _percent_col("true"),
        _percent_col("false"),
        (
            pl.when(pl.col("is_unique"))
            .then(pl.lit("true"))
            .otherwise(pl.lit(""))
            .alias("unique")
        ),
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
        print(str(compute_stats(df, changes_df=changes_df)), file=output)
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
    # MARK: pl.LazyFrame.collect
    changes_df = frame_diff(df_old.lazy(), df_new.lazy(), on=key).collect()
    return describe_frame(
        df_new,
        changes_df=changes_df,
        source=source,
        output=output,
    )


def update_parquet(
    filename: str,
    transform: Callable[[pl.LazyFrame], pl.LazyFrame],
    key: str,
) -> None:
    assert filename.endswith(".parquet")
    df = pl.read_parquet(filename)

    lf2 = transform(df.lazy())
    assert lf2.collect_schema() == df.schema, "schema changed"
    # MARK: pl.LazyFrame.collect
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


def now() -> pl.Expr:
    return pl.lit(datetime.datetime.now()).dt.round("1s").dt.cast_time_unit("ms")


def position_weights() -> pl.Expr:
    size = pl.len()
    row_nr = pl.int_range(1, size + 1)
    cumsum = (size * (size + 1)) / 2
    weights = row_nr / cumsum
    return weights.reverse()


def _weighted_random(s: pl.Series) -> pl.Series:
    size = len(s)
    values = np.random.choice(size, size=size, replace=False, p=s)
    return pl.Series(values=values, dtype=pl.UInt32)


def weighted_random(weights: pl.Expr) -> pl.Expr:
    # MARK: pl.Expr.map_batches
    return weights.map_batches(_weighted_random, return_dtype=pl.UInt32)


def weighted_sample[SomeFrame: (pl.DataFrame, pl.LazyFrame)](
    df: SomeFrame, n: int
) -> SomeFrame:
    weighted_args = position_weights().pipe(weighted_random)
    return df.sort(by=weighted_args).head(n=n)


def sample[SomeFrame: (pl.DataFrame, pl.LazyFrame)](
    df: SomeFrame,
    n: int | None = None,
    fraction: float | None = None,
    with_replacement: bool = False,
    shuffle: bool = False,
    seed: int | None = None,
) -> SomeFrame:
    def _sample(df: pl.DataFrame) -> pl.DataFrame:
        return df.sample(
            n=n,
            fraction=fraction,
            with_replacement=with_replacement,
            shuffle=shuffle,
            seed=seed,
        )

    return map_batches(df, _sample)


def head[SomeFrame: (pl.DataFrame, pl.LazyFrame)](
    df: SomeFrame, n: int | None
) -> SomeFrame:
    if n:
        return df.head(n)
    else:
        return df


class LimitWarning(Warning):
    pass


def limit[SomeFrame: (pl.DataFrame, pl.LazyFrame)](
    df: SomeFrame,
    n: int,
    sample: bool = True,
    desc: str = "frame",
) -> SomeFrame:
    def _inner(df: pl.DataFrame) -> pl.DataFrame:
        total = len(df)
        if total > n:
            warn(f"{desc} exceeded limit: {total:,}/{n:,}", LimitWarning)
            if sample:
                return df.sample(n)
            else:
                return df.head(n)
        else:
            return df

    return map_batches(df, _inner)


_limit = limit


def _align_to_index(df: pl.DataFrame, name: str) -> pl.DataFrame:
    if df.is_empty():
        return df

    dtype = df.schema[name]

    row = df.select(
        pl.col(name).max().cast(pl.Int64).alias("max"),
        pl.col(name).is_not_null().all().alias("not_null"),
        pl.col(name).is_unique().all().alias("unique"),
        pl.col(name).ge(0).all().alias("positive_int"),
    ).row(0, named=True)

    assert row["not_null"], f"column '{name}' has null values"
    assert row["unique"], f"column '{name}' has non-unique values"
    assert row["positive_int"], f"column '{name}' has negative values"
    assert row["max"] is not None, f"column '{name}' no max"

    id_df = (
        pl.int_range(end=row["max"] + 1, dtype=pl.UInt64, eager=True)
        .cast(dtype)
        .to_frame(name)
    )
    return id_df.join(df, on=name, how="left", coalesce=True).select(df.columns)


def align_to_index[SomeFrame: (pl.DataFrame, pl.LazyFrame)](
    df: SomeFrame, name: str
) -> SomeFrame:
    return map_batches(df, partial(_align_to_index, name=name))


def _update_or_append(df: pl.DataFrame, other: pl.DataFrame, on: str) -> pl.DataFrame:
    def _check_df(df: pl.DataFrame, df_label: str) -> None:
        row = df.select(
            pl.col(on).is_not_null().all().alias("not_null"),
            pl.col(on).is_unique().all().alias("unique"),
        ).row(0, named=True)

        assert row["not_null"], f"{df_label} '{on}' column has null values"
        assert row["unique"], f"{df_label} '{on}' column has non-unique values"

    other_cols = list(other.columns)
    other_cols.remove(on)

    _check_df(df=df, df_label="df")
    _check_df(df=other, df_label="other df")

    other = other.join(df.drop(other_cols), on=on, how="left", coalesce=True).select(
        df.columns
    )
    return pl.concat([df, other]).unique(subset=on, keep="last", maintain_order=True)


def update_or_append[SomeFrame: (pl.DataFrame, pl.LazyFrame)](
    df: SomeFrame,
    other: pl.DataFrame | pl.LazyFrame,
    on: str,
) -> SomeFrame:
    if isinstance(other, pl.LazyFrame):

        def _update_or_append_collecting(df: pl.DataFrame) -> pl.DataFrame:
            return _update_or_append(df, other.collect(), on)

        return map_batches(df, _update_or_append_collecting)
    else:
        return map_batches(df, partial(_update_or_append, other=other, on=on))


def _parse_csv_to_series(data: bytes, dtype: pl.Struct) -> pl.Series:
    return pl.read_csv(data, schema_overrides=dict(dtype)).to_struct("")


def csv_extract(
    expr: pl.Expr,
    dtype: PolarsDataType,
    log_group: str = "apply(csv_extract)",
) -> pl.Expr:
    assert isinstance(dtype, pl.List)
    inner_dtype = dtype.inner
    assert isinstance(inner_dtype, pl.Struct)
    return apply_with_tqdm(
        expr,
        partial(_parse_csv_to_series, dtype=inner_dtype),
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
    dtype: PolarsDataType,
    xpath: str = "./*",
    log_group: str = "apply(xml_extract)",
) -> pl.Expr:
    assert isinstance(dtype, pl.List)
    inner_dtype = dtype.inner
    assert isinstance(inner_dtype, pl.Struct)
    return apply_with_tqdm(
        expr,
        partial(_parse_xml_to_series, xpath=xpath, dtype=inner_dtype),
        return_dtype=dtype,
        log_group=log_group,
    )


def _xml_element_field_iter(
    element: ET.Element,
    name: str,
    dtype: PolarsDataType,
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
                if dtype == pl.Int64:
                    yield int(child.text)
                elif dtype == pl.Float64:
                    yield float(child.text)
                else:
                    yield child.text


def html_unescape(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        html.unescape,
        return_dtype=pl.Utf8,
        log_group="html_unescape",
    )


def _html_unescape_list(lst: list[str]) -> list[str]:
    return [html.unescape(s) for s in lst]


def html_unescape_list(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        _html_unescape_list,
        return_dtype=pl.List(pl.Utf8),
        log_group="html_unescape",
    )


def gzip_decompress(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        gzip.decompress,
        return_dtype=pl.Binary,
        log_group="gzip_decompress",
    )


def _zlib_decompress(data: bytes) -> str:
    return zlib.decompress(data, 16 + zlib.MAX_WBITS).decode("utf-8")


def zlib_decompress(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        _zlib_decompress,
        return_dtype=pl.Utf8,
        log_group="zlib_decompress",
    )


_RDF_STATEMENT_LIMIT = 250


def print_rdf_statements(
    df: pl.DataFrame | pl.LazyFrame,
    limit: int = _RDF_STATEMENT_LIMIT,
    sample: bool = True,
    file: TextIO = sys.stdout,
) -> None:
    assert df.collect_schema() == pl.Schema({"rdf_statement": pl.Utf8})
    df = df.pipe(_limit, limit, sample=sample, desc="rdf statements")

    if isinstance(df, pl.LazyFrame):
        # MARK: pl.LazyFrame.collect
        df = df.collect()

    for (line,) in df.iter_rows():
        print(line, file=file)


_TMPFILES: list[Path] = []


def _cleanup_tmpfiles() -> None:
    for tmpfile in _TMPFILES:
        tmpfile.unlink(missing_ok=True)


atexit.register(_cleanup_tmpfiles)


def scan_s3_parquet_anon(uri: str) -> pl.LazyFrame:
    assert uri.startswith("s3://")
    bucket, path = uri[5:].split("/", 1)
    url = f"https://{bucket}.s3.amazonaws.com/{path}"
    return pl.scan_parquet(url)
