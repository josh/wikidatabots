from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather


def df_diff(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    on: str | None = None,
) -> tuple[int, int, int]:
    df3 = df1.merge(df2, indicator=True, how="outer")
    if on:
        df4 = df1.merge(df2, on=on, indicator=True, how="outer")
    else:
        df4 = df3
    added = (df4["_merge"] == "right_only").sum()
    removed = (df4["_merge"] == "left_only").sum()
    both_key = (df4["_merge"] == "both").sum()
    both_equal = (df3["_merge"] == "both").sum()
    updated = both_key - both_equal
    assert updated >= 0
    return (added, removed, updated)


def df_upsert(
    df: pd.DataFrame,
    df2: pd.DataFrame,
    on: str,
) -> pd.DataFrame:
    indices = df[on].isin(df2[on])
    df3 = df[~indices]
    return pd.concat([df3, df2], ignore_index=True)


def df_append_new(
    df: pd.DataFrame,
    df_new: pd.DataFrame,
    on: str,
) -> pd.DataFrame:
    existing_indices = df_new[on].isin(df[on])
    new_df = df_new[~existing_indices]
    return pd.concat([df, new_df], ignore_index=True)


def update_feather(
    path: str,
    handle: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    df = pd.read_feather(path)
    df = handle(df)
    df.to_feather(path)


def write_feather_with_index(df: pd.DataFrame, path: str) -> None:
    table = pa.Table.from_pandas(df, preserve_index=None)

    for field in table.schema:
        assert not field.name.startswith("__index_level_"), field.name

    feather.write_feather(table, path)
