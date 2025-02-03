import os

import polars as pl
import pytest

from wd_tmdb import (
    find_tmdb_ids_not_found,
    find_tmdb_ids_via_imdb_id,
    find_tmdb_ids_via_tvdb_id,
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
