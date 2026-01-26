import atexit
import datetime
import sys
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, TextIO, TypeVar

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


_COL_SUPPORTS_UNIQUE = (
    cs.binary() | cs.boolean() | cs.numeric() | cs.string() | cs.temporal()
)


def now() -> pl.Expr:
    return pl.lit(datetime.datetime.now()).dt.round("1s").dt.cast_time_unit("ms")


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
