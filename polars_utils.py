# pyright: strict

import os
import random
import warnings
from typing import Any, Callable

import polars as pl
import requests
from tqdm import tqdm

import actions

actions.install_warnings_hook()


def read_ipc(filename: str) -> pl.LazyFrame:
    try:
        # TODO: Use scan_ipc
        return pl.read_ipc(filename, memory_map=False).lazy()
    except:  # noqa: E722
        warnings.warn("arrow2 reader failed, falling back to pyarrow")
        return pl.read_ipc(filename, use_pyarrow=True, memory_map=False).lazy()


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
    df = df.cache()
    assert df.schema[name] in PL_INTEGERS
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


def unique_row_differences(
    df1: pl.LazyFrame, df2: pl.LazyFrame, on: str
) -> tuple[int, int, int]:
    # .cache() doesn't seem to work here
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
    s: pl.Series,
    func: Callable[[Any], Any],
    return_dtype: pl.PolarsDataType | None = None,
    skip_nulls: bool = True,
    desc: str | None = None,
) -> pl.Series:
    pbar = tqdm()
    pbar.desc = desc
    pbar.total = len(s)
    pbar.unit = "rows"

    def wrapped_func(item: Any) -> Any:
        pbar.update(1)
        return func(item)

    try:
        return s.apply(wrapped_func, return_dtype=return_dtype, skip_nulls=skip_nulls)
    finally:
        pbar.close()


def request_text(urls: pl.Series) -> pl.Series:
    assert urls.dtype == pl.Utf8, "series must be strings"

    session = requests.Session()

    def get_text(url: str) -> str:
        return session.get(url, timeout=5).text

    return apply_with_tqdm(urls, get_text, return_dtype=pl.Utf8, desc="Fetching URLs")
