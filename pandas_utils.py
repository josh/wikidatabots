from typing import Callable, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather

import actions

actions.install_warnings_hook()


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


def safe_row_concat(dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
    dfs = list(dfs)

    if len(dfs) == 0:
        return pd.DataFrame()

    expected_dtype = dfs[0].dtypes

    for i, df in enumerate(dfs):
        assert isinstance(
            df.index, pd.RangeIndex
        ), "only RangeIndex can be concatenated"

        assert df.dtypes.equals(
            expected_dtype
        ), f"expected:\n{expected_dtype}\nbut at index {i} got:\n{df.dtypes}"

    df = pd.concat(dfs, ignore_index=True, verify_integrity=True)

    assert isinstance(df.index, pd.RangeIndex)
    assert df.dtypes.equals(expected_dtype)

    return df


def safe_column_join(dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
    dfs = list(dfs)

    row_count = None

    columns = set()
    for i, df in enumerate(dfs):
        if not row_count:
            row_count = len(df)
        else:
            assert (
                len(df) == row_count
            ), f"expected {row_count} rows but got {len(df)} at index {i}"

        for column in df.columns:
            assert column not in columns, f"duplicate column {column} at index {i}"
            columns.add(column)

    df = pd.concat(dfs, axis=1)

    return df
