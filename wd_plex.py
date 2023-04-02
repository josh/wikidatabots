# pyright: strict

import logging

import polars as pl

from plex_etl import GUID_TYPE, encode_plex_guids
from sparql import sparql_df

_LIMIT = 25


def _plex_guids() -> pl.LazyFrame:
    return (
        pl.scan_parquet(
            "s3://wikidatabots/plex.parquet",
            storage_options={"anon": True},
        )
        .select(["key", "type", "tmdb_id"])
        .pipe(encode_plex_guids)
    )


_TMDB_MOVIE_QUERY = """
SELECT DISTINCT ?item ?tmdb_id ?plex_guid WHERE {
  ?item wdt:P4947 ?tmdb_id.
  FILTER(xsd:integer(?tmdb_id))

  # Plex GUID / movie subject type constraints
  VALUES ?class {
    wd:Q11424 # film
    wd:Q24856 # film series
    wd:Q1261214 # television special
  }
  ?item (wdt:P31/(wdt:P279*)) ?class.

  OPTIONAL { ?item wdt:P11460 ?plex_guid. }
}
"""

_TMDB_TV_QUERY = """
SELECT DISTINCT ?item ?tmdb_id ?plex_guid WHERE {
  ?item wdt:P4983 ?tmdb_id.
  FILTER(xsd:integer(?tmdb_id))

  # Plex GUID / show subject type constraints
  VALUES ?class {
    wd:Q5398426 # television series
  }
  ?item (wdt:P31/(wdt:P279*)) ?class.

  OPTIONAL { ?item wdt:P11460 ?plex_guid. }
}
"""

_TMDB_QUERY: dict[GUID_TYPE, str] = {
    "movie": _TMDB_MOVIE_QUERY,
    "show": _TMDB_TV_QUERY,
}

_TMDB_QUERY_SCHEMA: dict[str, pl.PolarsDataType] = {
    "item": pl.Utf8,
    "tmdb_id": pl.UInt32,
    "plex_guid": pl.Utf8,
}


def _wikidata_tmdb_ids(guid_type: GUID_TYPE) -> pl.LazyFrame:
    return (
        sparql_df(_TMDB_QUERY[guid_type], schema=_TMDB_QUERY_SCHEMA)
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


def _rdf_statements(
    plex_df: pl.LazyFrame,
    guid_type: GUID_TYPE,
    source_label: str,
) -> pl.LazyFrame:
    df = plex_df.filter(pl.col("type") == guid_type).select(["guid", "tmdb_id"])
    return (
        _wikidata_tmdb_ids(guid_type)
        .join(df, on="tmdb_id")
        .select(_rdf_statement(source_label))
        .head(_LIMIT)
    )


def find_plex_guids_via_tmdb_id() -> pl.LazyFrame:
    plex_df = _plex_guids().cache()

    return pl.concat(
        [
            _rdf_statements(
                plex_df,
                guid_type="movie",
                source_label="TMDb movie ID",
            ),
            _rdf_statements(
                plex_df,
                guid_type="show",
                source_label="TMDb TV series ID",
            ),
        ],
        parallel=False,  # BUG: parallel caching is broken
    )


def main() -> None:
    df = find_plex_guids_via_tmdb_id()

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
