# pyright: strict

import os
import random
import re
import sys
import xml.etree.ElementTree as ET
from functools import reduce
from itertools import combinations
from math import ceil
from typing import Any, Callable, Iterable, Iterator, TypeVar

import polars as pl
from tqdm import tqdm


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


def head_mask(n: int) -> pl.Expr:
    expr = pl.arange(
        low=0,
        high=pl.count(),
    )
    assert isinstance(expr, pl.Expr)
    return expr < n


def is_constant(expr: pl.Expr) -> pl.Expr:
    return (expr == expr.first()).all()


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


def rank_sort(
    expr: pl.Expr,
    descending: bool = False,
    nulls_last: bool = False,
) -> pl.Expr:
    return expr.arg_sort(descending=descending, nulls_last=nulls_last).arg_sort()


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

    ldf = df.lazy().select(col_expr.values())
    orig_columns = ldf.columns

    for r in range(2, min(len(orig_columns), rmax + 1)):
        ldf = ldf.with_columns(
            _combine_col_expr(list(cols)) for cols in combinations(orig_columns, r)
        )

    df_total = ldf.select(pl.all().sum()).collect()

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


_POWERSET_COL_SEP = "--7C69799--"


def _combine_col_expr(cols: list[str]) -> pl.Expr:
    expr1 = pl.col(_POWERSET_COL_SEP.join(cols[:-1]))
    expr2 = pl.col(cols[-1])
    name = _POWERSET_COL_SEP.join(cols)
    return (expr1 & expr2).alias(name)


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
