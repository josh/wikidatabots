# pyright: strict

import datetime

import polars as pl
from polars.testing import assert_frame_equal

from tmdb_etl import fetch_tmdb_external_ids, insert_tmdb_latest_changes, tmdb_changes


def test_tmdb_changes():
    date = datetime.date(2023, 1, 1)
    tmdb_changes(date=date, tmdb_type="movie").collect()


def test_insert_tmdb_latest_changes():
    df1 = pl.DataFrame(
        {
            "id": [3],
            "has_changes": [True],
            "date": [datetime.date.today()],
            "adult": [False],
        },
        columns={
            "id": pl.UInt32,
            "has_changes": pl.Boolean,
            "date": pl.Date,
            "adult": pl.Boolean,
        },
    ).lazy()
    df2 = insert_tmdb_latest_changes(df1, tmdb_type="movie").collect()
    assert len(df2) > 0


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
