# pyright: strict

import os
import random
import sys
import xml.etree.ElementTree as ET
from typing import Any, Callable, Iterator, TypeVar

import polars as pl
from tqdm import tqdm

from actions import install_warnings_hook

install_warnings_hook()


def update_ipc(
    filename: str,
    transform: Callable[[pl.LazyFrame], pl.LazyFrame],
) -> None:
    df = pl.scan_ipc(filename, memory_map=False)
    df2 = transform(df)
    assert df2.schema == df.schema, "schema changed"
    tmpfile = f"{filename}.{random.randint(0, 2**32)}"
    # sink_ipc not yet supported in standard engine
    # df2.sink_ipc(tmpfile, compression="lz4")
    df2.collect().write_ipc(tmpfile, compression="lz4")
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
            low=0,
            high=pl.coalesce([pl.col(name).max().cast(pl.Int64) + 1, 0]),
            dtype=df.schema[name],
        ).alias(name)
    ).join(df, on=name, how="left")


def row_differences(df1: pl.LazyFrame, df2: pl.LazyFrame) -> tuple[int, int]:
    count_colname = "__count"
    count_col = pl.col(count_colname)

    count_agg_expr = pl.count().alias(count_colname).cast(pl.Int32)
    lf1x = df1.groupby(pl.all(), maintain_order=False).agg(count_agg_expr)
    lf2x = df2.groupby(pl.all(), maintain_order=False).agg(count_agg_expr)

    diff_counts = count_col.fill_null(0) - pl.col(f"{count_colname}_right").fill_null(0)
    sum_negative_count = (
        pl.when(count_col < 0).then(count_col.abs()).otherwise(0).sum().alias("removed")
    )
    sum_positive_count = (
        pl.when(count_col > 0).then(count_col).otherwise(0).sum().alias("added")
    )

    stats = (
        lf1x.join(lf2x, on=df1.columns, how="outer")
        .select(diff_counts)
        .select([sum_negative_count, sum_positive_count])
        .collect()
    )

    return stats[0, "removed"], stats[0, "added"]


def update_or_append(df: pl.LazyFrame, other: pl.LazyFrame, on: str) -> pl.LazyFrame:
    assert_expr = pl.col(on).is_not_null() & pl.col(on).is_unique()
    df = df.pipe(assert_expression, assert_expr, f"Bad '{on}' column on df").cache()
    other = other.pipe(
        assert_expression, assert_expr, f"Bad '{on}' column on other"
    ).cache()

    other_cols = list(other.columns)
    other_cols.remove(on)

    other = other.join(df.drop(other_cols), on=on, how="left").select(df.columns)
    return pl.concat(
        [df, other],
        parallel=False,  # BUG: parallel caching is broken
    ).unique(subset=on, keep="last")


def unique_row_differences(
    df1: pl.LazyFrame, df2: pl.LazyFrame, on: str
) -> tuple[int, int, int]:
    # FIXME: cache() isn't working
    # df1, df2 = df1.cache(), df2.cache()
    df1, df2 = df1.collect().lazy(), df2.collect().lazy()
    [removed, added, both_key, both_equal] = pl.collect_all(
        [
            df1.join(df2, on=on, how="anti"),
            df2.join(df1, on=on, how="anti"),
            df1.join(df2, on=on, how="semi"),
            df1.join(df2, on=df2.columns, how="semi"),
        ]
    )
    updated = both_key.height - both_equal.height
    return added.height, removed.height, updated


def apply_with_tqdm(
    expr: pl.Expr,
    function: Callable[[Any], Any],
    return_dtype: pl.PolarsDataType | None = None,
    desc: str | None = None,
    log_group: str = "apply(unknown)",
) -> pl.Expr:
    def apply_function(s: pl.Series) -> Iterator[Any]:
        if len(s) == 0:
            return

        try:
            print(f"::group::{log_group}", file=sys.stderr)
            for item in tqdm(s, desc=desc, unit="row"):
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
