import pandas as pd
import pytest

from pandas_utils import (
    compact_dtype,
    compact_dtypes,
    df_append_new,
    df_diff,
    df_upsert,
    is_dtype_pyarrow_lossless,
    read_json_series,
    reindex_as_range,
    safe_column_join,
    safe_row_concat,
    update_feather,
    write_feather_with_index,
)


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
    df3 = df_upsert(df1, df2, on="a")
    assert df3["a"].tolist() == [1, 2, 3, 4]


def test_df_append_new():
    df1 = pd.DataFrame({"a": [1, 2, 3], "b": [1, 1, 1]})
    df2 = pd.DataFrame({"a": [2, 3, 4], "b": [2, 2, 2]})
    df3 = df_append_new(df1, df2, on="a")
    assert df3["a"].tolist() == [1, 2, 3, 4]
    assert df3["b"].tolist() == [1, 1, 1, 2]

    df1 = df1.astype({"b": "Int64"})
    df2 = pd.DataFrame({"a": [2, 3, 4]})
    df3 = df_append_new(df1, df2, on="a")
    assert df3["a"].tolist() == [1, 2, 3, 4]
    assert df3["b"].tolist() == [1, 1, 1, pd.NA]


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


def test_write_feather_with_index():
    path = "/tmp/test.feather"
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": ["a", "b", "c"]})
    write_feather_with_index(df, path)

    df = pd.read_feather(path)
    assert df.index.dtype == "int64"
    assert df.columns.tolist() == ["a", "b", "c"]
    assert df["a"].dtype == "int64"
    assert df["b"].dtype == "int64"
    assert df["c"].dtype == "object"

    df = df.set_index("c")
    write_feather_with_index(df, path)

    df = pd.read_feather(path)
    assert df.index.dtype == "object"
    assert df.columns.tolist() == ["a", "b"]
    assert df["a"].dtype == "int64"
    assert df["b"].dtype == "int64"


def test_is_dtype_pyarrow_lossless():
    df = pd.DataFrame({"a": [1, 2, 3]})
    assert is_dtype_pyarrow_lossless(df)

    df = pd.DataFrame({"a": [1, 2, 3]}, dtype="Int32")
    assert not is_dtype_pyarrow_lossless(df)

    df = pd.DataFrame({"a": ["a", "b", "c"]})
    assert is_dtype_pyarrow_lossless(df)

    df = pd.DataFrame({"a": ["a", "b", "c"]}, dtype="string")
    assert not is_dtype_pyarrow_lossless(df)


def test_safe_row_concat():
    df1 = pd.DataFrame({"a": [1, 2, 3]})
    df2 = pd.DataFrame({"a": [4, 5, 6]})

    dfx = safe_row_concat([df1, df2])
    assert dfx.dtypes["a"] == "int64"
    assert dfx.index.tolist() == [0, 1, 2, 3, 4, 5]
    assert dfx["a"].tolist() == [1, 2, 3, 4, 5, 6]

    df2 = pd.DataFrame({"b": [7, 8, 9]})
    with pytest.raises(AssertionError):
        safe_row_concat([df1, df2])

    df2 = pd.DataFrame({"a": [4, 5, 6]}, index=["a", "b", "c"])
    with pytest.raises(AssertionError):
        safe_row_concat([df1, df2])

    df2 = pd.DataFrame({"a": [4, 5, pd.NA]})
    with pytest.raises(AssertionError):
        safe_row_concat([df1, df2])


def test_safe_column_join():
    df1 = pd.DataFrame({"a": [1, 2, 3]})
    df2 = pd.DataFrame({"b": [4, 5, 6]})

    dfx = safe_column_join([df1, df2])
    assert dfx.dtypes["a"] == "int64"
    assert dfx.dtypes["b"] == "int64"
    assert dfx.index.tolist() == [0, 1, 2]
    assert dfx["a"].tolist() == [1, 2, 3]
    assert dfx["b"].tolist() == [4, 5, 6]

    with pytest.raises(AssertionError):
        safe_column_join([df1, df1])

    df2 = pd.DataFrame({"b": [4, 5]})
    with pytest.raises(AssertionError):
        safe_column_join([df1, df2])


def test_reindex_as_range():
    index = pd.Index([1, 2, 4], name="id", dtype="uint8")
    df1 = pd.DataFrame({"a": [1, 2, 3]}, index=index)
    df2 = reindex_as_range(df1)
    assert len(df2) == 5
    assert isinstance(df2.index, pd.RangeIndex)
    assert df2.index.name == "id"
    assert df2.index.dtype == "int64"
    assert df2.loc[1, "a"] == df1.loc[1, "a"]
    assert df2.loc[2, "a"] == df1.loc[2, "a"]
    assert df2.loc[4, "a"] == df1.loc[4, "a"]


def test_series_compact_dtype():
    s1 = pd.Series([1, 2, 3])
    assert s1.dtype == "int64"
    s2 = compact_dtype(s1)
    assert s2.dtype == "uint8", f"dtype was {s2.dtype}"
    assert s1.to_list() == s2.to_list()

    s1 = pd.Series([10_000])
    assert s1.dtype == "int64"
    s2 = compact_dtype(s1)
    assert s2.dtype == "uint16", f"dtype was {s2.dtype}"
    assert s1.to_list() == s2.to_list()

    s1 = pd.Series([1_000_000])
    assert s1.dtype == "int64"
    s2 = compact_dtype(s1)
    assert s2.dtype == "uint32", f"dtype was {s2.dtype}"
    assert s1.to_list() == s2.to_list()

    s1 = pd.Series([1_000_000_000, -1])
    assert s1.dtype == "int64"
    s2 = compact_dtype(s1)
    assert s2.dtype == "int32", f"dtype was {s2.dtype}"
    assert s1.to_list() == s2.to_list()

    s1 = pd.Series([10_000, None], dtype="Int64")
    assert s1.dtype == "Int64"
    s2 = compact_dtype(s1)
    assert s2.dtype == "UInt16", f"dtype was {s2.dtype}"
    assert s1.to_list() == s2.to_list()

    s1 = pd.Series([1.0, 2.1, 3.0], dtype="Float64")
    assert s1.dtype == "Float64"
    s2 = compact_dtype(s1)
    assert s2.dtype == "float32", f"dtype was {s2.dtype}"

    s1 = pd.Series([1.0, 2.1, None])
    assert s1.dtype == "float64"
    s2 = compact_dtype(s1)
    assert s2.dtype == "Float32", f"dtype was {s2.dtype}"

    s1 = pd.Series([True, False])
    assert s1.dtype == "bool"
    s2 = compact_dtype(s1)
    assert s2.dtype == "bool", f"dtype was {s2.dtype}"

    s1 = pd.Series([True, False, None])
    assert s1.dtype == "object"
    s2 = compact_dtype(s1)
    assert s2.dtype == "boolean", f"dtype was {s2.dtype}"

    s1 = pd.Series(["foo", "bar"])
    assert s1.dtype == "object"
    s2 = compact_dtype(s1)
    assert s2.dtype == "string", f"dtype was {s2.dtype}"

    s1 = pd.Series(["foo", "bar", None])
    assert s1.dtype == "object"
    s2 = compact_dtype(s1)
    assert s2.dtype == "string", f"dtype was {s2.dtype}"


def test_compact_dtypes():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [10_000, 20_000, 30_000],
            "c": [{"a": 1}, {"a": 2}, {"a": 3}],
        }
    )
    assert df.dtypes["a"] == "int64"
    assert df.dtypes["b"] == "int64"
    assert df.dtypes["c"] == "object"

    df = compact_dtypes(df)
    assert df.dtypes["a"] == "uint8"
    assert df.dtypes["b"] == "uint16"
    assert df.dtypes["c"] == "object"


def test_read_json_series():
    text = '[{"a": 1}, {"a": 2}, {"a": 3}]'
    s = pd.Series(['{"a": 1}', '{"a": 2}', '{"a": 3}'], dtype="string")
    df1 = pd.read_json(text)
    df2 = read_json_series(s)
    assert df1.equals(df2)

    text = '[{"a": 1}, {"a": 2}, {"a": 3}]'
    s = pd.Series(['{"a": 1}', '{"a": 2}', '{"a": 3}'])
    df1 = pd.read_json(text, dtype={"a": "int8"})  # type: ignore
    df2 = read_json_series(s, dtype={"a": "int8"})
    assert df1.equals(df2)

    s = pd.Series(
        ['{"a": 1}', '{"a": 2}', '{"a": 3}'],
        index=["a", "b", "c"],
        dtype="string",
    )
    df = read_json_series(s)
    assert df.loc["a", "a"] == 1
    assert df.loc["b", "a"] == 2
    assert df.loc["c", "a"] == 3
