# pyright: basic

from typing import Callable

import fsspec
import pandas as pd


def df_diff(df1: pd.DataFrame, df2: pd.DataFrame, key: str) -> tuple[int, int, int]:
    df1_both_indices = df1[key].isin(df2[key])
    df2_both_indices = df2[key].isin(df1[key])

    df1_both = df1[df1_both_indices]
    df2_both = df2[df2_both_indices]
    df3_both = df1_both.merge(df2_both, indicator=True, how="left")

    added = (~df2_both_indices).sum()
    removed = (~df1_both_indices).sum()
    updated = (df3_both["_merge"] == "left_only").sum()

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
