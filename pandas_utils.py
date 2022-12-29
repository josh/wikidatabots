# pyright: basic

from typing import Callable

import pandas as pd

try:
    import fsspec
except ImportError:
    pass


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
    key: str,
) -> pd.DataFrame:
    indices = df[key].isin(df2[key])
    df3 = df[~indices]
    return pd.concat([df3, df2], ignore_index=True)


def update_feather(
    urlpath: str,
    handle: Callable[[pd.DataFrame], pd.DataFrame],
) -> None:
    with fsspec.open(urlpath, mode="rb") as f:
        df = pd.read_feather(f)  # type: ignore
    df = handle(df)
    with fsspec.open(urlpath, mode="wb") as f:
        df.to_feather(f)  # type: ignore
