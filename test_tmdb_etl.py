# pyright: strict

import datetime

import polars as pl
from polars.testing import assert_frame_equal

from tmdb_etl import (
    fetch_tmdb_external_ids,
    insert_tmdb_latest_changes,
    tmdb_changes,
    tmdb_exists,
    tmdb_find,
)


def test_fetch_tmdb_external_ids():
    ids = pl.LazyFrame({"id": [1, 2, 3, 4]})
    df = fetch_tmdb_external_ids(ids, "movie")
    df2 = pl.LazyFrame(
        {
            "id": [1, 2, 3, 4],
            "success": [False, True, True, False],
            "imdb_id": [None, "tt0094675", "tt0092149", None],
        }
    )
    assert df.schema == {
        "id": pl.Int64,
        "success": pl.Boolean,
        "retrieved_at": pl.Datetime(time_unit="ns"),
        "imdb_id": pl.Utf8,
        "tvdb_id": pl.UInt32,
        "imdb_numeric_id": pl.UInt32,
        "wikidata_numeric_id": pl.UInt32,
    }
    assert_frame_equal(df.select(["id", "success", "imdb_id"]), df2)


CHANGES_SCHEMA = {
    "id": pl.UInt32,
    "has_changes": pl.Boolean,
    "date": pl.Date,
    "adult": pl.Boolean,
}


def test_insert_tmdb_latest_changes():
    df1 = pl.LazyFrame(
        {
            "id": [3],
            "has_changes": [True],
            "date": [datetime.date.today()],
            "adult": [False],
        },
        schema=CHANGES_SCHEMA,
    )
    ldf = insert_tmdb_latest_changes(df1, tmdb_type="movie")
    assert ldf.schema == CHANGES_SCHEMA
    df2 = ldf.collect()
    assert len(df2) > 0


def test_tmdb_changes():
    dates_df = pl.LazyFrame(
        {"date": [datetime.date(2023, 1, 1), datetime.date(2023, 1, 2)]}
    )
    ldf = tmdb_changes(dates_df, tmdb_type="movie")
    assert ldf.schema == CHANGES_SCHEMA
    ldf.collect()


def test_tmdb_exists():
    df = pl.LazyFrame({"tmdb_id": [0, 2, 3, 4]})
    df2 = df.with_columns(tmdb_exists(tmdb_type="movie"))
    df3 = df.with_columns(pl.Series("exists", [False, True, True, False]))
    assert_frame_equal(df2, df3)


def test_find_by_external_id():
    df = pl.LazyFrame({"imdb_id": ["tt1630029", "tt14269590", "nm3718007"]})

    df2 = df.with_columns(tmdb_find(tmdb_type="movie", external_id_type="imdb_id"))
    df3 = df.with_columns(pl.Series("tmdb_id", [76600, None, None], dtype=pl.UInt32))
    assert_frame_equal(df2, df3)

    df2 = df.with_columns(tmdb_find(tmdb_type="tv", external_id_type="imdb_id"))
    df3 = df.with_columns(pl.Series("tmdb_id", [None, 120998, None], dtype=pl.UInt32))
    assert_frame_equal(df2, df3)

    df2 = df.with_columns(tmdb_find(tmdb_type="person", external_id_type="imdb_id"))
    df3 = df.with_columns(pl.Series("tmdb_id", [None, None, 1674162], dtype=pl.UInt32))
    assert_frame_equal(df2, df3)
