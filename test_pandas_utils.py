# pyright: basic

import pandas as pd

from pandas_utils import df_diff, df_upsert, update_feather


def test_df_diff():
    df1 = pd.DataFrame({"a": [1, 2, 3]})
    df2 = pd.DataFrame({"a": [2, 3, 4]})
    added, removed, updated = df_diff(df1, df2)
    assert added == 1
    assert removed == 1
    assert updated == 0

    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"a": [1, 2, 3, 4]})
    added, removed, updated = df_diff(df1, df2)
    assert added == 3
    assert removed == 0
    assert updated == 0

    df1 = pd.DataFrame({"a": [1, 2, 3, 4]})
    df2 = pd.DataFrame({"a": [1]})
    added, removed, updated = df_diff(df1, df2)
    assert added == 0
    assert removed == 3
    assert updated == 0

    df1 = pd.DataFrame({"a": [1, 2, 3], "b": [False, False, False]})
    df2 = pd.DataFrame({"a": [2, 3, 4], "b": [True, False, False]})
    added, removed, updated = df_diff(df1, df2, on="a")
    assert added == 1
    assert removed == 1
    assert updated == 1

    df1 = pd.DataFrame({"a": [1, 2, 3], "b": [False, False, False]})
    df2 = pd.DataFrame({"a": [1, 2, 3], "b": [True, True, False]})
    added, removed, updated = df_diff(df1, df2, on="a")
    assert added == 0
    assert removed == 0
    assert updated == 2


def test_df_upsert():
    df1 = pd.DataFrame({"a": [1, 2, 3]})
    df2 = pd.DataFrame({"a": [2, 3, 4]})
    df3 = df_upsert(df1, df2, key="a")
    assert df3["a"].tolist() == [1, 2, 3, 4]


def test_update_feather():
    path = "/tmp/test.feather"

    df = pd.DataFrame({"a": [1, 2, 3]})
    df.to_feather(path)

    def handle(df: pd.DataFrame) -> pd.DataFrame:
        df["b"] = [4, 5, 6]
        return df

    update_feather(path, handle)

    df = pd.read_feather(path)
    assert df["a"].tolist() == [1, 2, 3]
    assert df["b"].tolist() == [4, 5, 6]
