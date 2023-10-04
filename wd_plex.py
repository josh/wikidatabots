# pyright: strict

from datetime import datetime

import polars as pl

from plex_etl import GUID_TYPE, decode_plex_guid_key
from polars_utils import enable_string_cache, print_rdf_statements
from sparql import sparql


def _plex_guids() -> pl.LazyFrame:
    return pl.scan_parquet(
        "s3://wikidatabots/plex.parquet",
        storage_options={"anon": True},
    ).select("type", "tmdb_id", "key", "success", "retrieved_at")


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
        sparql(_TMDB_QUERY[guid_type], schema=_TMDB_QUERY_SCHEMA)
        .filter(pl.col("tmdb_id").is_unique() & pl.col("plex_guid").is_null())
        .drop("plex_guid")
        .with_columns(pl.lit(guid_type).cast(pl.Categorical).alias("type"))
    )


def _wikidata_all_tmdb_ids() -> pl.LazyFrame:
    return pl.concat([_wikidata_tmdb_ids("movie"), _wikidata_tmdb_ids("show")])


_RDF_STATEMENT = pl.format(
    '<{}> wdt:P11460 "{}" ; '
    'wikidatabots:editSummary "Add Plex key via associated {}" .',
    pl.col("item"),
    pl.col("hexkey"),
    pl.col("source_label"),
).alias("rdf_statement")

_FRESH_METADATA = pl.col("retrieved_at").dt.offset_by("2w") >= datetime.now()


def find_plex_guids_via_tmdb_id() -> pl.LazyFrame:
    return (
        _wikidata_all_tmdb_ids()
        .join(_plex_guids(), on=["type", "tmdb_id"], how="inner")
        .with_columns(
            pl.col("key").bin.encode("hex").alias("hexkey"),
        )
        .with_columns(
            pl.when(pl.col("type") == "movie")
            .then(pl.lit("TMDb movie ID"))
            .when(pl.col("type") == "show")
            .then(pl.lit("TMDb TV series ID"))
            .otherwise(None)
            .alias("source_label")
        )
        .filter(_FRESH_METADATA)
        .select(_RDF_STATEMENT)
    )


_LEGACY_FORMAT_QUERY = """
SELECT ?statement ?guid WHERE {
  ?statement ps:P11460 ?guid.
  FILTER(STRSTARTS(?guid, "plex://"))
}
"""

_RDF_MIGRATE_STATEMENT = pl.format(
    '<{}> ps:P11460 "{}" ; ' 'wikidatabots:editSummary "Migrate Plex key format" .',
    pl.col("statement"),
    pl.col("hexkey"),
).alias("rdf_statement")


def find_plex_guids_in_legacy_format() -> pl.LazyFrame:
    return (
        sparql(_LEGACY_FORMAT_QUERY, schema={"statement": pl.Utf8, "guid": pl.Utf8})
        .with_columns(
            pl.col("guid").pipe(decode_plex_guid_key).bin.encode("hex").alias("hexkey")
        )
        .filter(pl.col("hexkey").is_not_null())
        .select(_RDF_MIGRATE_STATEMENT)
    )


_NOT_DEPRECATED_QUERY = """
SELECT ?statement ?hexkey WHERE {
  ?statement ps:P11460 ?hexkey.
  ?statement wikibase:rank ?rank.
  FILTER(?rank != wikibase:DeprecatedRank)
}
"""

_DEPRECATE_RDF_STATEMENT = pl.format(
    "<{}> wikibase:rank wikibase:DeprecatedRank ; pq:P2241 wd:Q21441764 ; "
    'wikidatabots:editSummary "{}" .',
    pl.col("statement"),
    pl.lit("Deprecate removed Plex ID"),
).alias("rdf_statement")


def find_plex_keys_not_found() -> pl.LazyFrame:
    plex_df = _plex_guids().with_columns(
        pl.col("key").bin.encode("hex").alias("hexkey")
    )

    return (
        sparql(_NOT_DEPRECATED_QUERY, columns=["statement", "hexkey"])
        .join(plex_df, on="hexkey", how="left")
        .filter(_FRESH_METADATA & pl.col("success").not_())
        .select(_DEPRECATE_RDF_STATEMENT)
    )


def _main() -> None:
    enable_string_cache()

    pl.concat(
        [
            find_plex_guids_in_legacy_format(),
            find_plex_guids_via_tmdb_id(),
            find_plex_keys_not_found(),
        ]
    ).pipe(print_rdf_statements)


if __name__ == "__main__":
    _main()
