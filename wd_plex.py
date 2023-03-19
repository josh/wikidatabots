# pyright: strict

import logging

import polars as pl

from plex_etl import encode_plex_guids
from sparql import sparql_df

_LIMIT = 25


def _plex_guids() -> tuple[pl.LazyFrame, pl.LazyFrame]:
    df = (
        pl.scan_ipc(
            "s3://wikidatabots/plex.arrow",
            storage_options={"anon": True},
        )
        .select(["key", "type", "tmdb_id"])
        .pipe(encode_plex_guids)
        .cache()
    )
    movie_df = df.filter(pl.col("type") == "movie").select(["guid", "tmdb_id"])
    show_df = df.filter(pl.col("type") == "show").select(["guid", "tmdb_id"])
    return movie_df, show_df


_TMDB_QUERY = """
SELECT DISTINCT ?item ?tmdb_id ?plex_guid WHERE {
  ?item wdt:P0000 ?tmdb_id.
  FILTER(xsd:integer(?tmdb_id))

  VALUES ?classes {
    wd:Q15416
    wd:Q11424
    wd:Q24856
    wd:Q2431196
  }
  ?item (wdt:P31/(wdt:P279*)) ?classes.

  OPTIONAL { ?item wdt:P11460 ?plex_guid. }
}
"""

_TMDB_QUERY_SCHEMA: dict[str, pl.PolarsDataType] = {
    "item": pl.Utf8,
    "tmdb_id": pl.UInt32,
    "plex_guid": pl.Utf8,
}


def _wikidata_tmdb_ids(pid: str) -> pl.LazyFrame:
    return (
        sparql_df(_TMDB_QUERY.replace("P0000", pid), schema=_TMDB_QUERY_SCHEMA)
        .filter(pl.col("tmdb_id").is_unique() & pl.col("plex_guid").is_null())
        .drop("plex_guid")
    )


def _rdf_statement(source_label: str) -> pl.Expr:
    return pl.format(
        '<{}> wdt:P11460 "{}" ; wikidatabots:editSummary "{}" .',
        pl.col("item"),
        pl.col("guid"),
        pl.lit(f"Add Plex GUID claim via associated {source_label}"),
    ).alias("rdf_statement")


def find_plex_guids_via_tmdb_id() -> pl.LazyFrame:
    plex_movie_df, plex_show_df = _plex_guids()

    wd_movie_df = (
        _wikidata_tmdb_ids("P4947")
        .join(plex_movie_df, on="tmdb_id")
        .select(_rdf_statement(source_label="TMDb movie ID"))
        .head(_LIMIT)
    )
    wd_tv_df = (
        _wikidata_tmdb_ids("P4983")
        .join(plex_show_df, on="tmdb_id")
        .select(_rdf_statement(source_label="TMDb TV series ID"))
        .head(_LIMIT)
    )
    return pl.concat(
        [wd_movie_df, wd_tv_df],
        parallel=False,  # BUG: parallel caching is broken
    )


def main() -> None:
    df = find_plex_guids_via_tmdb_id()

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
