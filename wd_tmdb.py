# pyright: strict

import logging
from typing import Literal

import polars as pl

from sparql import sparql_df
from tmdb_etl import EXTRACT_IMDB_NUMERIC_ID, TMDB_TYPE, tmdb_exists, tmdb_find

STATEMENT_LIMIT = 100
TMDB_ID_PID = Literal["P4947", "P4983", "P4985"]

TMDB_TYPE_TO_WD_PID: dict[TMDB_TYPE, TMDB_ID_PID] = {
    "movie": "P4947",
    "tv": "P4983",
    "person": "P4985",
}

WD_PID_LABEL: dict[TMDB_ID_PID, str] = {
    "P4947": "TMDb movie ID",
    "P4983": "TMDb TV series ID",
    "P4985": "TMDb person ID",
}

MOVIE_IMDB_QUERY = """
SELECT ?item ?imdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  VALUES ?classes {
    wd:Q11424
    wd:Q1261214
  }
  ?item (wdt:P31/(wdt:P279*)) ?classes.

  OPTIONAL { ?item wdt:P4947 ?tmdb_id. }
  FILTER(!(BOUND(?tmdb_id)))
}
"""

TV_IMDB_QUERY = """
SELECT ?item ?imdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  VALUES ?classes {
    wd:Q15416
  }
  ?item (wdt:P31/(wdt:P279*)) ?classes.

  OPTIONAL { ?item p:P4983 ?tmdb_id. }
  FILTER(!(BOUND(?tmdb_id)))
}
"""

PERSON_IMDB_QUERY = """
SELECT ?item ?imdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  ?item wdt:P31 wd:Q5.

  OPTIONAL { ?item wdt:P4985 ?tmdb_id. }
  FILTER(!(BOUND(?tmdb_id)))
}
"""

IMDB_QUERY: dict[TMDB_ID_PID, str] = {
    "P4947": MOVIE_IMDB_QUERY,
    "P4983": TV_IMDB_QUERY,
    "P4985": PERSON_IMDB_QUERY,
}


def find_tmdb_ids_via_imdb_id(tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    wd_pid = TMDB_TYPE_TO_WD_PID[tmdb_type]
    sparql_query = IMDB_QUERY[wd_pid]

    rdf_statement = pl.format(
        '<{}> wdt:{} "{}" ; wikidatabots:editSummary "{}" .',
        pl.col("item"),
        pl.lit(wd_pid),
        pl.col("tmdb_id"),
        pl.lit(f"Add {WD_PID_LABEL[wd_pid]} claim via associated IMDb ID"),
    ).alias("rdf_statement")

    tmdb_df = (
        pl.scan_ipc(f"s3://wikidatabots/tmdb/{tmdb_type}/external_ids.arrow")
        .select(["id", "imdb_numeric_id"])
        .rename({"id": "tmdb_id"})
        .drop_nulls()
        .unique(subset=["imdb_numeric_id"])
    )

    wd_df = (
        sparql_df(sparql_query, columns=["item", "imdb_id"])
        .with_columns(EXTRACT_IMDB_NUMERIC_ID[tmdb_type])
        .drop_nulls()
    )

    return (
        wd_df.join(tmdb_df, on="imdb_numeric_id", how="left")
        .drop_nulls()
        .select(["item", "imdb_id"])
        .with_columns(tmdb_find(tmdb_type=tmdb_type, external_id_type="imdb_id"))
        .select(["item", "tmdb_id"])
        .drop_nulls()
        .select(rdf_statement)
    )


TV_TVDB_QUERY = """
SELECT ?item ?tvdb_id WHERE {
  ?item wdt:P4835 ?tvdb_id.

  VALUES ?classes {
    wd:Q15416
  }
  ?item (wdt:P31/(wdt:P279*)) ?classes.

  FILTER(xsd:integer(?tvdb_id))

  OPTIONAL { ?item p:P4983 ?tmdb_id. }
  FILTER(!(BOUND(?tmdb_id)))
}
"""

TVDB_QUERY: dict[TMDB_ID_PID, str] = {
    "P4983": TV_TVDB_QUERY,
}


def find_tmdb_ids_via_tvdb_id(tmdb_type: Literal["tv"]) -> pl.LazyFrame:
    wd_pid = TMDB_TYPE_TO_WD_PID[tmdb_type]
    sparql_query = TVDB_QUERY[wd_pid]

    rdf_statement = pl.format(
        '<{}> wdt:{} "{}" ; wikidatabots:editSummary "{}" .',
        pl.col("item"),
        pl.lit(wd_pid),
        pl.col("tmdb_id"),
        pl.lit(
            f"Add {WD_PID_LABEL[wd_pid]} claim via associated TheTVDB.com series ID"
        ),
    ).alias("rdf_statement")

    tmdb_df = (
        pl.scan_ipc(f"s3://wikidatabots/tmdb/{tmdb_type}/external_ids.arrow")
        .select(["id", "tvdb_id"])
        .rename({"id": "tmdb_id"})
        .drop_nulls()
        .unique(subset=["tvdb_id"])
    )

    wd_df = sparql_df(
        sparql_query, schema={"item": pl.Utf8, "tvdb_id": pl.UInt32}
    ).drop_nulls()

    return (
        wd_df.join(tmdb_df, on="tvdb_id", how="left")
        .drop_nulls()
        .select(["item", "tvdb_id"])
        .with_columns(tmdb_find(tmdb_type=tmdb_type, external_id_type="tvdb_id"))
        .select(["item", "tmdb_id"])
        .drop_nulls()
        .select(rdf_statement)
    )


NOT_DEPRECATED_QUERY = """
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

    changes_df = pl.scan_ipc(f"s3://wikidatabots/tmdb/{tmdb_type}/latest_changes.arrow")

    query = NOT_DEPRECATED_QUERY.replace("P0000", TMDB_TYPE_TO_WD_PID[tmdb_type])
    df = sparql_df(query, schema={"statement": pl.Utf8, "id": pl.UInt32})

    return (
        df.join(changes_df, on="id", how="left")
        .filter(pl.col("adult").is_null() & pl.col("has_changes"))
        .rename({"id": "tmdb_id"})
        .filter(tmdb_exists(tmdb_type).is_not())
        .select(rdf_statement)
    )


def main() -> None:
    df = pl.concat(
        [
            find_tmdb_ids_via_imdb_id("movie"),
            find_tmdb_ids_via_imdb_id("tv"),
            find_tmdb_ids_via_tvdb_id("tv"),
            find_tmdb_ids_via_imdb_id("person"),
            find_tmdb_ids_not_found("movie"),
            find_tmdb_ids_not_found("tv"),
            find_tmdb_ids_not_found("person"),
        ]
    ).head(STATEMENT_LIMIT)

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
