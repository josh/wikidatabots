# pyright: strict

import polars as pl
import pytest
from hypothesis import given
from polars.testing import assert_frame_equal
from polars.testing.parametric import column, dataframes

from polars_utils import reindex_as_range, row_differences, unique_row_differences


def test_reindex_as_range():
    df1 = pl.DataFrame(
        {
            "id": [1, 2, 5],
            "value": [1, 2, 5],
        }
    )
    df2 = pl.DataFrame(
        {
            "id": [0, 1, 2, 3, 4, 5],
            "value": [None, 1, 2, None, None, 5],
        }
    )
    assert_frame_equal(reindex_as_range(df1, name="id"), df2)

    df = pl.DataFrame(
        {
            "id": [-1, 2, 5],
            "value": [-1, 2, 5],
        }
    )
    with pytest.raises(AssertionError):
        reindex_as_range(df, name="id")

    df = pl.DataFrame(
        {
            "id": ["1", "2", "5"],
            "value": [1, 2, 5],
        }
    )
    with pytest.raises(AssertionError):
        reindex_as_range(df, name="id")


def test_row_differences():
    df1 = pl.DataFrame({"a": [1, 2, 3]})
    df2 = pl.DataFrame({"a": [2, 3, 4]})
    added, removed = row_differences(df1, df2)
    assert added == 1
    assert removed == 1

    df1 = pl.DataFrame({"a": [1]})
    df2 = pl.DataFrame({"a": [1, 2, 3, 4]})
    added, removed = row_differences(df1, df2)
    assert added == 3
    assert removed == 0

    df1 = pl.DataFrame({"a": [1, 2, 3, 4]})
    df2 = pl.DataFrame({"a": [1]})
    added, removed = row_differences(df1, df2)
    assert added == 0
    assert removed == 3

    df1 = pl.DataFrame({"a": [1]})
    df2 = pl.DataFrame({"a": [1, 1]})
    with pytest.raises(AssertionError):
        added, removed = row_differences(df1, df2)
        # assert added == 1
        # assert removed == 0

    df1 = pl.DataFrame({"a": [1, 1]})
    df2 = pl.DataFrame({"a": [1]})
    with pytest.raises(AssertionError):
        added, removed = row_differences(df1, df2)
        # assert added == 0
        # assert removed == 1


df_st = dataframes(
    cols=[column("a", dtype=pl.Int64, unique=True), column("b", dtype=pl.Boolean)]
)


@given(df1=df_st, df2=df_st)
def test_row_differences_properties(df1: pl.DataFrame, df2: pl.DataFrame) -> None:
    added, removed = row_differences(df1, df2)
    assert added >= 0, "added should be >= 0"
    assert added <= len(df2), "added should be <= len(df2)"
    assert removed >= 0, "removed should be >= 0"
    assert removed <= len(df1), "removed should be <= len(df1)"
    assert df1.height - removed + added == df2.height, "df1 - removed + added == df2"
    assert df2.height - added + removed == df1.height, "df2 - added + removed == df1"


def test_unique_row_differences():
    df1 = pl.DataFrame({"a": [1, 2, 3], "b": [False, False, False]})
    df2 = pl.DataFrame({"a": [2, 3, 4], "b": [True, False, False]})
    added, removed, updated = unique_row_differences(df1, df2, on="a")
    assert added == 1
    assert removed == 1
    assert updated == 1

    df1 = pl.DataFrame({"a": [1, 2, 3], "b": [False, False, False]})
    df2 = pl.DataFrame({"a": [1, 2, 3], "b": [True, True, False]})
    added, removed, updated = unique_row_differences(df1, df2, on="a")
    assert added == 0
    assert removed == 0
    assert updated == 2
