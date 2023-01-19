import polars as pl
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
    df3 = reindex_as_range(df1, name="id")
    assert_frame_equal(df2, df3)
