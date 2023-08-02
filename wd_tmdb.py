# pyright: strict

from typing import Literal

import polars as pl

from polars_utils import print_rdf_statements
from sparql import sparql
from tmdb_etl import TMDB_TYPE, extract_imdb_numeric_id, tmdb_exists, tmdb_find
from wikidata import is_blocked_item

_TMDB_ID_PID = Literal["P4947", "P4983", "P4985"]

_TMDB_TYPE_TO_WD_PID: dict[TMDB_TYPE, _TMDB_ID_PID] = {
    "movie": "P4947",
    "tv": "P4983",
    "person": "P4985",
}

_WD_PID_LABEL: dict[_TMDB_ID_PID, str] = {
    "P4947": "TMDb movie ID",
    "P4983": "TMDb TV series ID",
    "P4985": "TMDb person ID",
}

_MOVIE_IMDB_QUERY = """
SELECT DISTINCT ?item ?imdb_id ?tmdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  # TMDb movie ID subject type constraints
  VALUES ?class {
    wd:Q11424 # film
    wd:Q24856 # film series
    wd:Q1261214 # television special
  }
  ?item (wdt:P31/(wdt:P279*)) ?class.

  OPTIONAL {
    ?item wdt:P4947 ?tmdb_id.
    FILTER(xsd:integer(?tmdb_id))
  }
}
"""

_TV_IMDB_QUERY = """
SELECT DISTINCT ?item ?imdb_id ?tmdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  # TMDb TV series ID subject type constraints
  VALUES ?class {
    wd:Q15416 # television program
    wd:Q5398426 # television series
  }
  ?item (wdt:P31/(wdt:P279*)) ?class.

  OPTIONAL {
    ?item wdt:P4983 ?tmdb_id.
    FILTER(xsd:integer(?tmdb_id))
  }
}
"""

_PERSON_IMDB_QUERY = """
SELECT DISTINCT ?item ?imdb_id ?tmdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  # TMDb person ID subject type constraints
  VALUES ?class {
    wd:Q5 # human
    wd:Q16334295 # group of humans
  }
  ?item (wdt:P31/(wdt:P279*)) ?class.
  # ?item wdt:P31 wd:Q5.

  OPTIONAL {
    ?item wdt:P4985 ?tmdb_id.
    FILTER(xsd:integer(?tmdb_id))
  }
}
"""

_IMDB_QUERY: dict[_TMDB_ID_PID, str] = {
    "P4947": _MOVIE_IMDB_QUERY,
    "P4983": _TV_IMDB_QUERY,
    "P4985": _PERSON_IMDB_QUERY,
}

_IMDB_QUERY_SCHEMA: dict[str, pl.PolarsDataType] = {
    "item": pl.Utf8,
    "imdb_id": pl.Utf8,
    "tmdb_id": pl.UInt32,
}


def find_tmdb_ids_via_imdb_id(tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    wd_pid = _TMDB_TYPE_TO_WD_PID[tmdb_type]
    sparql_query = _IMDB_QUERY[wd_pid]

    rdf_statement = pl.format(
        '<{}> wdt:{} "{}" ; wikidatabots:editSummary "{}" .',
        pl.col("item"),
        pl.lit(wd_pid),
        pl.col("tmdb_id"),
        pl.lit(f"Add {_WD_PID_LABEL[wd_pid]} claim via associated IMDb ID"),
    ).alias("rdf_statement")

    tmdb_df = (
        pl.read_parquet(
            f"s3://wikidatabots/tmdb/{tmdb_type}.parquet",
            columns=["id", "imdb_numeric_id"],
            storage_options={"anon": True},
        )
        .lazy()
        .rename({"id": "tmdb_id"})
        .drop_nulls()
        .unique(subset=["imdb_numeric_id"], maintain_order=True)
    )

    wd_df = (
        sparql(sparql_query, schema=_IMDB_QUERY_SCHEMA)
        .with_columns(pl.col("imdb_id").pipe(extract_imdb_numeric_id, tmdb_type))
        .filter(
            pl.col("imdb_numeric_id").is_unique()
            & pl.col("tmdb_id").is_null()
            & pl.col("item").pipe(is_blocked_item).is_not()
        )
        .drop("tmdb_id")
        .drop_nulls()
    )

    return (
        wd_df.join(tmdb_df, on="imdb_numeric_id", how="left")
        .drop_nulls()
        .select(["item", "imdb_id"])
        .with_columns(pl.col("imdb_id").pipe(tmdb_find, tmdb_type=tmdb_type))
        .select(["item", "tmdb_id"])
        .drop_nulls()
        .select(rdf_statement)
    )


_TV_TVDB_QUERY = """
SELECT DISTINCT ?item ?tvdb_id ?tmdb_id WHERE {
  ?item wdt:P4835 ?tvdb_id.

  # TMDb TV series ID subject type constraints
  VALUES ?class {
    wd:Q15416 # television program
    wd:Q5398426 # television series
  }
  ?item (wdt:P31/(wdt:P279*)) ?class.

  FILTER(xsd:integer(?tvdb_id))

  OPTIONAL {
    ?item wdt:P4983 ?tmdb_id.
    FILTER(xsd:integer(?tmdb_id))
  }
}
"""

_TVDB_QUERY: dict[_TMDB_ID_PID, str] = {
    "P4983": _TV_TVDB_QUERY,
}

_TVDB_QUERY_SCHEMA: dict[str, pl.PolarsDataType] = {
    "item": pl.Utf8,
    "tvdb_id": pl.UInt32,
    "tmdb_id": pl.UInt32,
}


def find_tmdb_ids_via_tvdb_id(tmdb_type: Literal["tv"]) -> pl.LazyFrame:
    wd_pid = _TMDB_TYPE_TO_WD_PID[tmdb_type]
    sparql_query = _TVDB_QUERY[wd_pid]

    rdf_statement = pl.format(
        '<{}> wdt:{} "{}" ; wikidatabots:editSummary "{}" .',
        pl.col("item"),
        pl.lit(wd_pid),
        pl.col("tmdb_id"),
        pl.lit(
            f"Add {_WD_PID_LABEL[wd_pid]} claim via associated TheTVDB.com series ID"
        ),
    ).alias("rdf_statement")

    tmdb_df = (
        pl.read_parquet(
            f"s3://wikidatabots/tmdb/{tmdb_type}.parquet",
            columns=["id", "tvdb_id"],
            storage_options={"anon": True},
        )
        .lazy()
        .rename({"id": "tmdb_id"})
        .drop_nulls()
        .unique(subset=["tvdb_id"], maintain_order=True)
    )

    wd_df = (
        sparql(sparql_query, schema=_TVDB_QUERY_SCHEMA)
        .filter(
            pl.col("tvdb_id").is_unique()
            & pl.col("tmdb_id").is_null()
            & pl.col("item").pipe(is_blocked_item).is_not()
        )
        .drop("tmdb_id")
        .drop_nulls()
    )

    return (
        wd_df.join(tmdb_df, on="tvdb_id", how="left")
        .drop_nulls()
        .select(["item", "tvdb_id"])
        .with_columns(pl.col("tvdb_id").pipe(tmdb_find, tmdb_type=tmdb_type))
        .select(["item", "tmdb_id"])
        .drop_nulls()
        .select(rdf_statement)
    )


_NOT_DEPRECATED_QUERY = """
SELECT ?statement ?id WHERE {
  ?statement ps:P0000 ?id.
  ?statement wikibase:rank ?rank.
  FILTER(?rank != wikibase:DeprecatedRank)
  FILTER(xsd:integer(?id))
}
"""


def find_tmdb_ids_not_found(
    tmdb_type: TMDB_TYPE,
) -> pl.LazyFrame:
    rdf_statement = pl.format(
        "<{}> wikibase:rank wikibase:DeprecatedRank ; pq:P2241 wd:Q21441764 ; "
        'wikidatabots:editSummary "{}" .',
        pl.col("statement"),
        pl.lit(f"Deprecate removed TMDB {tmdb_type} ID"),
    ).alias("rdf_statement")

    tmdb_df = pl.read_parquet(
        f"s3://wikidatabots/tmdb/{tmdb_type}.parquet",
        columns=["id", "date", "success"],
        storage_options={"anon": True},
    ).lazy()

    query = _NOT_DEPRECATED_QUERY.replace("P0000", _TMDB_TYPE_TO_WD_PID[tmdb_type])
    df = sparql(query, schema={"statement": pl.Utf8, "id": pl.UInt32})

    if tmdb_type == "movie":
        exists_expr = (
            tmdb_exists(pl.col("tmdb_id"), "movie")
            .or_(tmdb_exists(pl.col("tmdb_id"), "collection"))
            .alias("exists")
        )
    else:
        exists_expr = tmdb_exists(pl.col("tmdb_id"), tmdb_type).alias("exists")

    return (
        df.join(tmdb_df, on="id", how="left")
        .filter(pl.col("success").is_not())
        # .filter(pl.col("adult").is_null() & pl.col("date").is_not_null())
        .rename({"id": "tmdb_id"})
        .with_columns(exists_expr)
        .filter(pl.col("exists").is_not())
        .select(rdf_statement)
    )


def _main() -> None:
    pl.enable_string_cache(True)

    pl.concat(
        [
            find_tmdb_ids_via_imdb_id("movie"),
            find_tmdb_ids_via_imdb_id("tv"),
            find_tmdb_ids_via_tvdb_id("tv"),
            find_tmdb_ids_via_imdb_id("person"),
            find_tmdb_ids_not_found("movie"),
            find_tmdb_ids_not_found("tv"),
            find_tmdb_ids_not_found("person"),
        ]
    ).pipe(print_rdf_statements)


if __name__ == "__main__":
    _main()
