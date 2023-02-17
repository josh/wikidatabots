# pyright: strict

import logging

import polars as pl

from sparql import sparql_df
from tmdb_etl import EXTRACT_IMDB_TITLE_NUMERIC_ID, tmdb_find


def main():
    rdf_statement = pl.format(
        '<{}> wdt:P4947 "{}" ; wikidatabots:editSummary "{}" .',
        pl.col("item"),
        pl.col("tmdb_id"),
        pl.lit("Add TMDb movie ID claim via associated IMDb ID"),
    )

    tmdb_df = (
        pl.scan_ipc("s3://wikidatabots/tmdb/movie/external_ids.arrow")
        .select(["id", "imdb_numeric_id"])
        .rename({"id": "tmdb_id"})
        .drop_nulls()
        .unique(subset=["imdb_numeric_id"])
    )

    query = """
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

    wd_df = (
        sparql_df(query, columns=["item", "imdb_id"])
        .with_columns(EXTRACT_IMDB_TITLE_NUMERIC_ID)
        .drop_nulls()
    )

    df = (
        wd_df.join(tmdb_df, on="imdb_numeric_id", how="left")
        .drop_nulls()
        .select(["item", "imdb_id"])
        .with_columns(tmdb_find(tmdb_type="movie", external_id_type="imdb_id"))
        .select(["item", "tmdb_id"])
        .drop_nulls()
        .select(rdf_statement)
    )

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
