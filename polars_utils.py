# pyright: strict

import polars as pl


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
    [removed, added] = pl.collect_all(
        [
            lf1.join(lf2, on=df2.columns, how="anti"),
            lf2.join(lf1, on=df1.columns, how="anti"),
        ]
    )
    assert df1.height - removed.height + added.height == df2.height
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
