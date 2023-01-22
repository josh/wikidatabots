# pyright: strict

import polars as pl
import pytest
from hypothesis import given
from polars.testing import assert_frame_equal
from polars.testing.parametric import column, dataframes

from polars_utils import (
    apply_with_tqdm,
    lazy_apply_with_tqdm,
    reindex_as_range,
    row_differences,
    unique_row_differences,
)


def test_reindex_as_range():
    df1 = pl.DataFrame([], columns={"id": pl.Int64}).lazy()
    assert_frame_equal(reindex_as_range(df1, name="id"), df1)

    df1 = pl.DataFrame(
        {
            "id": [1, 2, 5],
            "value": [1, 2, 5],
        }
    ).lazy()
    df2 = pl.DataFrame(
        {
            "id": [0, 1, 2, 3, 4, 5],
            "value": [None, 1, 2, None, None, 5],
        }
    ).lazy()
    assert_frame_equal(reindex_as_range(df1, name="id"), df2)

    # df = pl.DataFrame(
    #     {
    #         "id": [-1, 2, 5],
    #         "value": [-1, 2, 5],
    #     }
    # ).lazy()
    # reindex_as_range(df, name="id").collect()

    df = pl.DataFrame(
        {
            "id": ["a", "b", "c"],
            "value": [1, 2, 5],
        }
    ).lazy()
    with pytest.raises(Exception):
        reindex_as_range(df, name="id").collect()


@given(
    df=dataframes(
        cols=[
            column("a", dtype=pl.UInt16, unique=True),
            column("b", dtype=pl.Boolean),
        ]
    )
)
def test_reindex_as_range_properties(df: pl.DataFrame):
    df2 = reindex_as_range(df.lazy(), name="a").collect()
    assert df2.height >= df.height


def test_row_differences():
    df1 = pl.DataFrame({"a": [1, 2, 3]}).lazy()
    df2 = pl.DataFrame({"a": [2, 3, 4]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 1
    assert removed == 1

    df1 = pl.DataFrame({"a": [1]}).lazy()
    df2 = pl.DataFrame({"a": [1, 2, 3, 4]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 3
    assert removed == 0

    df1 = pl.DataFrame({"a": [1, 2, 3, 4]}).lazy()
    df2 = pl.DataFrame({"a": [1]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 0
    assert removed == 3

    df1 = pl.DataFrame({"a": [1]}).lazy()
    df2 = pl.DataFrame({"a": [1, 1]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 1
    assert removed == 0

    df1 = pl.DataFrame({"a": [1, 1]}).lazy()
    df2 = pl.DataFrame({"a": [1]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 0
    assert removed == 1


df_st = dataframes(cols=[column("a", dtype=pl.Int64), column("b", dtype=pl.Boolean)])


@given(df1=df_st, df2=df_st)
def test_row_differences_properties(df1: pl.DataFrame, df2: pl.DataFrame) -> None:
    added, removed = row_differences(df1.lazy(), df2.lazy())
    assert added >= 0, "added should be >= 0"
    assert added <= len(df2), "added should be <= len(df2)"
    assert removed >= 0, "removed should be >= 0"
    assert removed <= len(df1), "removed should be <= len(df1)"
    assert df1.height - removed + added == df2.height, "df1 - removed + added == df2"
    assert df2.height - added + removed == df1.height, "df2 - added + removed == df1"


def test_unique_row_differences():
    df1 = pl.DataFrame({"a": [1, 2, 3], "b": [False, False, False]}).lazy()
    df2 = pl.DataFrame({"a": [2, 3, 4], "b": [True, False, False]}).lazy()
    added, removed, updated = unique_row_differences(df1, df2, on="a")
    assert added == 1
    assert removed == 1
    assert updated == 1

    df1 = pl.DataFrame({"a": [1, 2, 3], "b": [False, False, False]}).lazy()
    df2 = pl.DataFrame({"a": [1, 2, 3], "b": [True, True, False]}).lazy()
    added, removed, updated = unique_row_differences(df1, df2, on="a")
    assert added == 0
    assert removed == 0
    assert updated == 2


df_st = dataframes(
    cols=[column("a", dtype=pl.Int64, unique=True), column("b", dtype=pl.Boolean)]
)


@given(df1=df_st, df2=df_st)
def test_unique_row_differences_properties(df1: pl.DataFrame, df2: pl.DataFrame):
    added, removed, updated = unique_row_differences(df1.lazy(), df2.lazy(), on="a")
    assert added >= 0, "added should be >= 0"
    assert added <= len(df2), "added should be <= len(df2)"
    assert removed >= 0, "removed should be >= 0"
    assert removed <= len(df1), "removed should be <= len(df1)"
    assert updated >= 0, "updated should be >= 0"
    assert updated <= len(df1), "updated should be <= len(df1)"
    assert df1.height - removed + added == df2.height, "df1 - removed + added == df2"
    assert df2.height - added + removed == df1.height, "df2 - added + removed == df1"


def test_apply_with_tqdm():
    df1 = pl.DataFrame({"a": [1, 2, 3], "b": [False, False, True]})
    df2 = pl.DataFrame({"a": [2, 3, 4], "b": [True, True, False]})

    def apply_fn(row: tuple[int, bool]) -> tuple[int, bool]:
        a, b = row
        return (a + 1, not b)

    df3 = apply_with_tqdm(df1, apply_fn).rename({"column_0": "a", "column_1": "b"})
    assert_frame_equal(df3, df2)


def test_lazy_apply_with_tqdm():
    df1 = pl.DataFrame({"a": [1, 2, 3], "b": [False, False, True]}).lazy()
    df2 = pl.DataFrame({"a": [2, 3, 4], "b": [True, True, False]}).lazy()

    def apply_fn(row: tuple[int, bool]) -> tuple[int, bool]:
        a, b = row
        return (a + 1, not b)

    schema: dict[str, pl.PolarsDataType] = {
        "column_0": pl.Int64,
        "column_1": pl.Boolean,
    }
    df3 = lazy_apply_with_tqdm(df1, apply_fn, schema=schema).rename(
        {"column_0": "a", "column_1": "b"}
    )
    assert_frame_equal(df3, df2)
