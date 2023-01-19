# pyright: strict

import polars as pl
import pytest
from polars.testing import assert_frame_equal

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
