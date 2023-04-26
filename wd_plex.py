# pyright: strict

import logging

import polars as pl

from plex_etl import GUID_TYPE, encode_plex_guids
from polars_utils import limit
from sparql import sparql_df

_STATEMENT_LIMIT = 100


def _plex_guids() -> pl.LazyFrame:
    return (
        pl.scan_parquet(
            "s3://wikidatabots/plex.parquet",
            storage_options={"anon": True},
        )
        .select(["type", "tmdb_id", "key"])
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
        .with_columns(pl.lit(guid_type).cast(pl.Categorical).alias("type"))
    )


def _wikidata_all_tmdb_ids() -> pl.LazyFrame:
    return pl.concat([_wikidata_tmdb_ids("movie"), _wikidata_tmdb_ids("show")])


_RDF_STATEMENT = pl.format(
    '<{}> wdt:P11460 "{}" ; '
    'wikidatabots:editSummary "Add Plex GUID claim via associated {}" .',
    pl.col("item"),
    pl.col("guid"),
    pl.col("source_label"),
).alias("rdf_statement")


def find_plex_guids_via_tmdb_id() -> pl.LazyFrame:
    return (
        _wikidata_all_tmdb_ids()
        .join(_plex_guids(), on=["type", "tmdb_id"])
        .with_columns(
            pl.when(pl.col("type") == "movie")
            .then(pl.lit("TMDb movie ID"))
            .when(pl.col("type") == "show")
            .then(pl.lit("TMDb TV series ID"))
            .otherwise(None)
            .alias("source_label")
        )
        .select(_RDF_STATEMENT)
    )


def main() -> None:
    with pl.StringCache():
        df = find_plex_guids_via_tmdb_id().pipe(
            limit, soft=_STATEMENT_LIMIT, desc="rdf_statements"
        )

        for (line,) in df.collect().iter_rows():
            print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
