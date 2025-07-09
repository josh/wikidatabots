import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from wd_tmdb import (
    find_tmdb_ids_not_found,
    find_tmdb_ids_via_imdb_id,
    find_tmdb_ids_via_tvdb_id,
    tmdb_exists,
    tmdb_find,
)

TMDB_API_KEY = os.environ.get("TMDB_API_KEY")


@pytest.mark.skipif(TMDB_API_KEY is None, reason="Missing TMDB_API_KEY")
def test_find_tmdb_ids_via_imdb_id() -> None:
    df = find_tmdb_ids_via_imdb_id(tmdb_type="movie")
    assert df.collect_schema() == pl.Schema({"rdf_statement": pl.Utf8})


@pytest.mark.skipif(TMDB_API_KEY is None, reason="Missing TMDB_API_KEY")
def test_find_tmdb_ids_via_tvdb_id() -> None:
    df = find_tmdb_ids_via_tvdb_id(tmdb_type="tv")
    assert df.collect_schema() == pl.Schema({"rdf_statement": pl.Utf8})


@pytest.mark.skipif(TMDB_API_KEY is None, reason="Missing TMDB_API_KEY")
def test_find_tmdb_ids_not_found() -> None:
    df = find_tmdb_ids_not_found(tmdb_type="movie")
    assert df.collect_schema() == pl.Schema({"rdf_statement": pl.Utf8})


@pytest.mark.skipif(TMDB_API_KEY is None, reason="Missing TMDB_API_KEY")
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


@pytest.mark.skipif(TMDB_API_KEY is None, reason="Missing TMDB_API_KEY")
def test_find() -> None:
    df = pl.LazyFrame({"imdb_id": ["tt1630029", "tt14269590", "nm3718007"]})

    df2 = df.with_columns(
        pl.col("imdb_id").pipe(
            tmdb_find,
            tmdb_type="movie",
            external_id_type="imdb_id",
        ),
    )
    df3 = df.with_columns(pl.Series("tmdb_id", [76600, None, None], dtype=pl.UInt32))
    assert_frame_equal(df2, df3)

    df2 = df.with_columns(
        pl.col("imdb_id").pipe(
            tmdb_find,
            tmdb_type="tv",
            external_id_type="imdb_id",
        ),
    )
    df3 = df.with_columns(pl.Series("tmdb_id", [None, 120998, None], dtype=pl.UInt32))
    assert_frame_equal(df2, df3)

    df2 = df.with_columns(
        pl.col("imdb_id").pipe(
            tmdb_find,
            tmdb_type="person",
            external_id_type="imdb_id",
        )
    )
    df3 = df.with_columns(pl.Series("tmdb_id", [None, None, 1674162], dtype=pl.UInt32))
    assert_frame_equal(df2, df3)
