import polars as pl

from wd_plex import find_plex_guids_via_tmdb_id


def test_find_plex_guids_via_tmdb_id() -> None:
    ldf = find_plex_guids_via_tmdb_id()
    assert ldf.collect_schema() == pl.Schema({"rdf_statement": pl.Utf8})
