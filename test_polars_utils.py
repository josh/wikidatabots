import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_utils import reindex_as_range


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
