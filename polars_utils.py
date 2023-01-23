# pyright: strict

import warnings
from typing import Any, Callable

import polars as pl
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


def reindex_as_range(df: pl.LazyFrame, name: str) -> pl.LazyFrame:
    assert df.schema[name] in PL_INTEGERS
    return df.select(
        pl.arange(
            low=0,
            high=pl.col(name).max().cast(pl.Int64).fill_null(-1) + 1,
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

    return stats["removed"][0], stats["added"][0]


def unique_row_differences(
    df1: pl.LazyFrame, df2: pl.LazyFrame, on: str
) -> tuple[int, int, int]:
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
    df: pl.DataFrame,
    fn: Callable[[tuple[Any, ...]], Any],
    return_dtype: pl.PolarsDataType | None = None,
    inference_size: int = 256,
) -> pl.DataFrame:
    pbar = tqdm()
    pbar.total = df.height

    def wrapped_fn(row: tuple[Any, ...]) -> Any:
        pbar.update(1)
        return fn(row)

    try:
        return df.apply(
            wrapped_fn, return_dtype=return_dtype, inference_size=inference_size
        )
    finally:
        pbar.close()


def lazy_apply_with_tqdm(
    df: pl.LazyFrame,
    fn: Callable[[tuple[Any, ...]], Any],
    schema: dict[str, pl.PolarsDataType] | None = None,
) -> pl.LazyFrame:
    return df.map(lambda df: apply_with_tqdm(df, fn), schema=schema)
