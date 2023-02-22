# pyright: strict

import polars as pl

from wd_tmdb import (
    find_tmdb_ids_not_found,
    find_tmdb_ids_via_imdb_id,
    find_tmdb_ids_via_tvdb_id,
)


def test_find_tmdb_ids_via_imdb_id() -> None:
    df = find_tmdb_ids_via_imdb_id(tmdb_type="movie")
    assert df.schema == {"rdf_statement": pl.Utf8}


def test_find_tmdb_ids_via_tvdb_id() -> None:
    df = find_tmdb_ids_via_tvdb_id(tmdb_type="tv")
    assert df.schema == {"rdf_statement": pl.Utf8}


def test_find_tmdb_ids_not_found() -> None:
    df = find_tmdb_ids_not_found(tmdb_type="movie")
    assert df.schema == {"rdf_statement": pl.Utf8}
