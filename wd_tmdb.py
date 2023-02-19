# pyright: strict

from typing import Literal

import polars as pl

from sparql import sparql_df
from tmdb_etl import EXTRACT_IMDB_NUMERIC_ID, TMDB_TYPE, tmdb_find

STATEMENT_LIMIT = 100
TMDB_ID_PID = Literal["P4947", "P4983", "P4985"]


def find_tmdb_ids_via_imdb_id(
    tmdb_type: TMDB_TYPE,
    sparql_query: str,
    wd_pid: TMDB_ID_PID,
    wd_plabel: str,
) -> pl.LazyFrame:
    rdf_statement = pl.format(
        '<{}> wdt:{} "{}" ; wikidatabots:editSummary "{}" .',
        pl.col("item"),
        pl.lit(wd_pid),
        pl.col("tmdb_id"),
        pl.lit(f"Add {wd_plabel} claim via associated IMDb ID"),
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
        .head(STATEMENT_LIMIT)
    )


def find_tmdb_ids_via_tvdb_id(
    tmdb_type: Literal["tv"],
    sparql_query: str,
    wd_pid: Literal["P4983"],
    wd_plabel: str,
) -> pl.LazyFrame:
    rdf_statement = pl.format(
        '<{}> wdt:{} "{}" ; wikidatabots:editSummary "{}" .',
        pl.col("item"),
        pl.lit(wd_pid),
        pl.col("tmdb_id"),
        pl.lit(f"Add {wd_plabel} claim via associated TheTVDB.com series ID"),
    ).alias("rdf_statement")

    tmdb_df = (
        pl.scan_ipc(f"s3://wikidatabots/tmdb/{tmdb_type}/external_ids.arrow")
        .select(["id", "tvdb_id"])
        .rename({"id": "tmdb_id"})
        .drop_nulls()
        .unique(subset=["tvdb_id"])
    )

    wd_df = sparql_df(
        sparql_query, dtypes={"item": pl.Utf8, "tvdb_id": pl.UInt32}
    ).drop_nulls()

    return (
        wd_df.join(tmdb_df, on="tvdb_id", how="left")
        .drop_nulls()
        .select(["item", "tvdb_id"])
        .with_columns(tmdb_find(tmdb_type=tmdb_type, external_id_type="tvdb_id"))
        .select(["item", "tmdb_id"])
        .drop_nulls()
        .select(rdf_statement)
        .head(STATEMENT_LIMIT)
    )
