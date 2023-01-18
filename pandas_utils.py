from typing import Callable, Iterable

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


NULLABLE_DTYPES = {
    "int8": "Int8",
    "int16": "Int16",
    "int32": "Int32",
    "int64": "Int64",
    "uint8": "UInt8",
    "uint16": "UInt16",
    "uint32": "UInt32",
    "uint64": "UInt64",
    "float32": "Float32",
    "float64": "Float64",
    "bool": "boolean",
}

NON_NULLABLE_DTYPES = {v: k for k, v in NULLABLE_DTYPES.items()}


def compact_dtype(s: pd.Series) -> pd.Series:
    inferred_dtype = pd.api.types.infer_dtype(s, skipna=True)

    if inferred_dtype == "string":
        s = s.astype("string")
    elif inferred_dtype == "floating":
        s = pd.to_numeric(s, downcast="float")
    elif inferred_dtype == "integer":
        if s.min() >= 0:
            s = pd.to_numeric(s, downcast="unsigned")
        else:
            s = pd.to_numeric(s, downcast="signed")
    elif inferred_dtype == "boolean":
        s = s.astype("boolean")

    if s.hasnans and s.dtype.name in NULLABLE_DTYPES:
        s = s.astype(NULLABLE_DTYPES[s.dtype.name])
    elif not s.hasnans and s.dtype.name in NON_NULLABLE_DTYPES:
        s = s.astype(NON_NULLABLE_DTYPES[s.dtype.name])

    return s


def compact_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(compact_dtype)
