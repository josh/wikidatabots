# pyright: strict

import polars as pl

from wd_tmdb import find_tmdb_ids_via_imdb_id, find_tmdb_ids_via_tvdb_id


def test_find_tmdb_ids_via_imdb_id() -> None:
    df = find_tmdb_ids_via_imdb_id(
        tmdb_type="movie",
        sparql_query="???",
        wd_pid="P4947",
        wd_plabel="TMDb movie ID",
    )
    assert df.schema == {"rdf_statement": pl.Utf8}


def test_find_tmdb_ids_via_tvdb_id() -> None:
    df = find_tmdb_ids_via_tvdb_id(
        tmdb_type="tv",
        sparql_query="???",
        wd_pid="P4983",
        wd_plabel="TMDb TV series ID",
    )
    assert df.schema == {"rdf_statement": pl.Utf8}
