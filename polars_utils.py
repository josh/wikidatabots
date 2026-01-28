import datetime
import sys
from collections.abc import Callable
from typing import Any, TextIO, TypeVar

import polars as pl
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
