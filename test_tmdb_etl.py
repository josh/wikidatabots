# pyright: strict

import datetime

import polars as pl
from polars.testing import assert_frame_equal

from tmdb_etl import (
    insert_tmdb_external_ids,
    insert_tmdb_latest_changes,
    tmdb_changes,
    tmdb_exists,
    tmdb_export,
    tmdb_external_ids,
    tmdb_find,
)

_SCHEMA: dict[str, pl.PolarsDataType] = {
    "id": pl.UInt32,
    "date": pl.Date,
    "adult": pl.Boolean,
    "in_export": pl.Boolean,
    "success": pl.Boolean,
    "retrieved_at": pl.Datetime(time_unit="ns"),
    "imdb_numeric_id": pl.UInt32,
    "tvdb_id": pl.UInt32,
    "wikidata_numeric_id": pl.UInt32,
}


def test_insert_tmdb_external_ids() -> None:
    df1 = pl.LazyFrame(
        {
            "id": [3],
            "date": [datetime.date.today()],
            "adult": [False],
            "in_export": [True],
            "success": [None],
            "retrieved_at": [None],
            "imdb_numeric_id": [None],
            "tvdb_id": [None],
            "wikidata_numeric_id": [None],
        },
        schema=_SCHEMA,
    )
    ldf = insert_tmdb_external_ids(df1, tmdb_type="movie")
    assert ldf.schema == _SCHEMA
    df2 = ldf.collect()
    assert len(df2) > 0


def test_insert_tmdb_latest_changes() -> None:
    df1 = pl.LazyFrame(
        {
            "id": [3],
            "date": [datetime.date.today()],
            "adult": [False],
            "in_export": [True],
            "success": [None],
            "retrieved_at": [None],
            "imdb_numeric_id": [None],
            "tvdb_id": [None],
            "wikidata_numeric_id": [None],
        },
        schema=_SCHEMA,
    )
    ldf = insert_tmdb_latest_changes(df1, tmdb_type="movie")
    assert ldf.schema == _SCHEMA
    df2 = ldf.collect()
    assert len(df2) > 0


def test_tmdb_changes() -> None:
    dates_df = pl.LazyFrame(
        {"date": [datetime.date(2023, 1, 1), datetime.date(2023, 1, 2)]}
    )
    ldf = tmdb_changes(dates_df, tmdb_type="movie")
    assert ldf.schema == {
        "id": pl.UInt32,
        "date": pl.Date,
        "adult": pl.Boolean,
    }
    ldf.collect()


def test_tmdb_exists() -> None:
    df = pl.LazyFrame({"tmdb_id": [0, 2, 3, 4, 3106]})
    df2 = df.with_columns(pl.col("tmdb_id").pipe(tmdb_exists, "movie"))
    df3 = df.with_columns(pl.Series("exists", [False, True, True, False, False]))
    assert_frame_equal(df2, df3)

    df = pl.LazyFrame({"tmdb_id": []})
    df2 = df.with_columns(pl.col("tmdb_id").pipe(tmdb_exists, "movie"))
    df3 = df.with_columns(pl.Series("exists", [], dtype=pl.Boolean))
    assert_frame_equal(df2, df3)

    df = pl.LazyFrame({"tmdb_id": [2, 87255]})
    df2 = df.with_columns(pl.col("tmdb_id").pipe(tmdb_exists, "collection"))
    df3 = df.with_columns(pl.Series("exists", [False, True]))
    assert_frame_equal(df2, df3)


def test_tmdb_external_ids() -> None:
    ids = pl.Series("id", [1, 2, 3, 4], dtype=pl.UInt32)
    df = tmdb_external_ids(ids.to_frame().lazy(), tmdb_type="movie")
    df2 = pl.LazyFrame(
        {
            "id": ids,
            "success": [False, True, True, False],
            "imdb_numeric_id": pl.Series([None, 94675, 92149, None], dtype=pl.UInt32),
        }
    )
    assert df.schema == {
        "id": pl.UInt32,
        "success": pl.Boolean,
        "retrieved_at": pl.Datetime(time_unit="ns"),
        "imdb_numeric_id": pl.UInt32,
        "tvdb_id": pl.UInt32,
        "wikidata_numeric_id": pl.UInt32,
    }
    assert_frame_equal(df.select(["id", "success", "imdb_numeric_id"]), df2)


def test_find() -> None:
    df = pl.LazyFrame({"imdb_id": ["tt1630029", "tt14269590", "nm3718007"]})

    df2 = df.with_columns(pl.col("imdb_id").pipe(tmdb_find, tmdb_type="movie"))
    df3 = df.with_columns(pl.Series("tmdb_id", [76600, None, None], dtype=pl.UInt32))
    assert_frame_equal(df2, df3)

    df2 = df.with_columns(pl.col("imdb_id").pipe(tmdb_find, tmdb_type="tv"))
    df3 = df.with_columns(pl.Series("tmdb_id", [None, 120998, None], dtype=pl.UInt32))
    assert_frame_equal(df2, df3)

    df2 = df.with_columns(pl.col("imdb_id").pipe(tmdb_find, tmdb_type="person"))
    df3 = df.with_columns(pl.Series("tmdb_id", [None, None, 1674162], dtype=pl.UInt32))
    assert_frame_equal(df2, df3)


def test_tmdb_export() -> None:
    df = tmdb_export("tv").collect()
    assert len(df) > 0
