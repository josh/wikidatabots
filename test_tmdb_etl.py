# pyright: strict

import datetime

import polars as pl
from polars.testing import assert_frame_equal

from tmdb_etl import (
    CHANGES_SCHEMA,
    fetch_tmdb_external_ids,
    insert_tmdb_latest_changes,
    tmdb_changes,
    tmdb_find_by_external_id,
)


def test_tmdb_changes():
    dates_df = pl.DataFrame(
        {"date": [datetime.date(2023, 1, 1), datetime.date(2023, 1, 2)]}
    ).lazy()

    ldf = tmdb_changes(dates_df, tmdb_type="movie")
    assert ldf.schema == CHANGES_SCHEMA
    ldf.collect()


def test_insert_tmdb_latest_changes():
    df1 = pl.DataFrame(
        {
            "id": [3],
            "has_changes": [True],
            "date": [datetime.date.today()],
            "adult": [False],
        },
        schema=CHANGES_SCHEMA,
    ).lazy()
    ldf = insert_tmdb_latest_changes(df1, tmdb_type="movie")
    assert ldf.schema == CHANGES_SCHEMA
    df2 = ldf.collect()
    assert len(df2) > 0


def test_fetch_tmdb_external_ids():
    ids = pl.DataFrame({"id": [1, 2, 3, 4]}).lazy()
    df = fetch_tmdb_external_ids(ids, "movie").select(["id", "success", "imdb_id"])
    df2 = pl.DataFrame(
        {
            "id": pl.Series([1, 2, 3, 4], dtype=pl.UInt32),
            "success": [False, True, True, False],
            "imdb_id": [None, "tt0094675", "tt0092149", None],
        }
    ).lazy()
    assert_frame_equal(df, df2)


def test_tmdb_find_by_external_id():
    df = pl.DataFrame({"imdb_id": ["tt1630029", "tt14269590", "nm3718007"]}).lazy()

    df2 = tmdb_find_by_external_id(df, tmdb_type="movie", external_id_type="imdb_id")
    df3 = df.with_columns(pl.Series("tmdb_id", [76600, None, None]))
    assert_frame_equal(df2, df3)

    df2 = tmdb_find_by_external_id(df, tmdb_type="tv", external_id_type="imdb_id")
    df3 = df.with_columns(pl.Series("tmdb_id", [None, 120998, None]))
    assert_frame_equal(df2, df3)

    df2 = tmdb_find_by_external_id(df, tmdb_type="person", external_id_type="imdb_id")
    df3 = df.with_columns(pl.Series("tmdb_id", [None, None, 1674162]))
    assert_frame_equal(df2, df3)
