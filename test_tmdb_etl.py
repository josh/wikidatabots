import datetime

import polars as pl
from polars.testing import assert_frame_equal

from tmdb_etl import (
    fetch_tmdb_external_ids,
    insert_tmdb_changes,
    tmdb_changes,
    tmdb_latest_changes,
)


def test_tmdb_changes():
    date = datetime.date(2023, 1, 1)
    tmdb_changes(date=date, tmdb_type="movie")


def test_insert_tmdb_changes():
    df1 = pl.DataFrame(
        {
            "date": [datetime.date.today()],
            "id": [3],
            "adult": [False],
        },
        columns={"date": pl.Date, "id": pl.UInt32, "adult": pl.Boolean},
    )
    df2 = insert_tmdb_changes(df1, tmdb_type="movie", days=0)
    assert len(df2) > 0


def test_tmdb_latest_changes():
    df1 = pl.DataFrame(
        {
            "date": [
                datetime.date.today(),
                datetime.date.today(),
                datetime.date.today(),
            ],
            "id": [1, 2, 1],
            "adult": [False, False, True],
        },
        columns={"date": pl.Date, "id": pl.UInt32, "adult": pl.Boolean},
    )
    df2 = pl.DataFrame(
        {
            "id": [0, 1, 2],
            "has_changes": [False, True, True],
            "date": [None, datetime.date.today(), datetime.date.today()],
            "adult": [None, True, False],
        },
        columns={
            "id": pl.UInt32,
            "has_changes": pl.Boolean,
            "date": pl.Date,
            "adult": pl.Boolean,
        },
    )
    df3 = tmdb_latest_changes(df1)
    assert_frame_equal(df2, df3)


def test_fetch_tmdb_external_ids():
    ids = pl.Series([1, 2, 3, 4])
    df = fetch_tmdb_external_ids(ids, "movie").select(["id", "success", "imdb_id"])
    df2 = pl.DataFrame(
        {
            "id": pl.Series([1, 2, 3, 4], dtype=pl.UInt32),
            "success": [False, True, True, False],
            "imdb_id": [None, "tt0094675", "tt0092149", None],
        }
    )
    assert_frame_equal(df, df2)
