# pyright: strict

import warnings

import polars as pl

import actions

actions.install_warnings_hook()


def read_ipc(filename: str):
    try:
        return pl.read_ipc(filename, memory_map=False)
    except:  # noqa: E722
        warnings.warn("arrow2 reader failed, falling back to pyarrow")
        return pl.read_ipc(filename, use_pyarrow=True, memory_map=False)


def reindex_as_range(df: pl.DataFrame, name: str) -> pl.DataFrame:
    col = df[name]
    lower, upper = col.min(), col.max()
    assert isinstance(lower, int) and isinstance(upper, int)
    assert lower >= 0
    values = range(upper + 1)
    index = pl.Series(name=name, values=values, dtype=col.dtype)
    return index.to_frame().join(df, on=name, how="left")


def row_differences(df1: pl.DataFrame, df2: pl.DataFrame) -> tuple[int, int]:
    lf1, lf2 = df1.lazy(), df2.lazy()
    [removed, added, unique1, unique2] = pl.collect_all(
        [
            lf1.join(lf2, on=df2.columns, how="anti"),
            lf2.join(lf1, on=df1.columns, how="anti"),
            lf1.unique(),
            lf2.unique(),
        ]
    )
    # TODO: duplicate values aren't handled correctly
    assert unique1.height == df1.height, "df1 rows must be unique"
    assert unique2.height == df2.height, "df2 rows must be unique"
    return added.height, removed.height


def unique_row_differences(
    df1: pl.DataFrame,
    df2: pl.DataFrame,
    on: str,
) -> tuple[int, int, int]:
    lf1, lf2 = df1.lazy(), df2.lazy()
    [removed, added, both_key, both_equal] = pl.collect_all(
        [
            lf1.join(lf2, on=on, how="anti"),
            lf2.join(lf1, on=on, how="anti"),
            lf1.join(lf2, on=on, how="semi"),
            lf1.join(lf2, on=df2.columns, how="semi"),
        ]
    )
    assert df1.height - removed.height + added.height == df2.height
    updated = both_key.height - both_equal.height
    assert updated >= 0
    return added.height, removed.height, updated
