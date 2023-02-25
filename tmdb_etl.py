# pyright: strict

import datetime
import os
from typing import Literal

import backoff
import polars as pl
import requests

from polars_utils import align_to_index, series_apply_with_tqdm, timestamp, update_ipc

session = requests.Session()

TMDB_TYPE = Literal["movie", "tv", "person"]

ONE_DAY = datetime.timedelta(days=1)

EXTRACT_IMDB_TITLE_NUMERIC_ID = (
    pl.col("imdb_id")
    .str.extract(r"tt(\d+)", 1)
    .cast(pl.UInt32)
    .alias("imdb_numeric_id")
)
EXTRACT_IMDB_NAME_NUMERIC_ID = (
    pl.col("imdb_id")
    .str.extract(r"nm(\d+)", 1)
    .cast(pl.UInt32)
    .alias("imdb_numeric_id")
)
EXTRACT_IMDB_NUMERIC_ID: dict[TMDB_TYPE, pl.Expr] = {
    "movie": EXTRACT_IMDB_TITLE_NUMERIC_ID,
    "tv": EXTRACT_IMDB_TITLE_NUMERIC_ID,
    "person": EXTRACT_IMDB_NAME_NUMERIC_ID,
}

EXTRACT_WIKIDATA_NUMERIC_ID = (
    pl.col("wikidata_id")
    .str.extract(r"Q(\d+)", 1)
    .cast(pl.UInt32)
    .alias("wikidata_numeric_id")
)

EXTERNAL_IDS_RESPONSE_DTYPE = pl.Struct(
    {
        "success": pl.Boolean,
        "id": pl.Int64,
        "imdb_id": pl.Utf8,
        "tvdb_id": pl.UInt32,
        "wikidata_id": pl.Utf8,
    }
)


def fetch_tmdb_external_ids(
    tmdb_ids: pl.LazyFrame, tmdb_type: TMDB_TYPE
) -> pl.LazyFrame:
    return (
        tmdb_ids.with_columns(
            pl.format(
                "https://api.themoviedb.org/3/{}/{}/external_ids?api_key={}",
                pl.lit(tmdb_type),
                pl.col("id"),
                pl.lit(os.environ["TMDB_API_KEY"]),
            )
            .map(_request_text, return_dtype=pl.Utf8)
            .str.json_extract(dtype=EXTERNAL_IDS_RESPONSE_DTYPE)
            .alias("result")
        )
        .with_columns(
            pl.col("result").struct.field("success").alias("success"),
            pl.col("result").struct.field("imdb_id").alias("imdb_id"),
            pl.col("result").struct.field("tvdb_id").alias("tvdb_id"),
            pl.col("result").struct.field("wikidata_id").alias("wikidata_id"),
        )
        .select(
            pl.col("id"),
            pl.col("success").fill_null(True),
            timestamp().alias("retrieved_at"),
            pl.col("imdb_id"),
            pl.col("tvdb_id"),
            EXTRACT_IMDB_NUMERIC_ID[tmdb_type],
            EXTRACT_WIKIDATA_NUMERIC_ID,
        )
    )


def insert_tmdb_latest_changes(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    df = df.cache()
    dates_df = df.select(
        [
            pl.date_range(
                low=pl.col("date").max().alias("start_date") - ONE_DAY,
                high=datetime.date.today(),
                interval="1d",
            ).alias("date")
        ]
    )

    return (
        pl.concat([df, tmdb_changes(dates_df, tmdb_type=tmdb_type)])
        .unique(subset="id", keep="last")
        .pipe(align_to_index, name="id")
        .with_columns(pl.col("has_changes").fill_null(False))
    )


CHANGES_RESPONSE_DTYPE = pl.Struct(
    {"results": pl.List(pl.Struct({"id": pl.Int64, "adult": pl.Boolean}))}
)


def tmdb_changes(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    return (
        df.with_columns(
            pl.format(
                "https://api.themoviedb.org/3/{}/changes"
                "?api_key={}&start_date={}&end_date={}",
                pl.lit(tmdb_type),
                pl.lit(os.environ["TMDB_API_KEY"]),
                pl.col("date"),
                (pl.col("date") + ONE_DAY),
            ).alias("url")
        )
        .select(
            [
                pl.col("date"),
                pl.col("url")
                .map(_request_text, return_dtype=pl.Utf8)
                .str.json_extract(dtype=CHANGES_RESPONSE_DTYPE)
                .struct.field("results")
                .arr.reverse()
                .alias("results"),
            ]
        )
        .explode("results")
        .select(
            [
                pl.col("results").struct.field("id").alias("id").cast(pl.UInt32),
                pl.lit(True).alias("has_changes"),
                pl.col("date"),
                pl.col("results").struct.field("adult").alias("adult"),
            ]
        )
    )


def tmdb_exists(tmdb_type: TMDB_TYPE) -> pl.Expr:
    return (
        pl.format(
            "https://api.themoviedb.org/3/{}/{}?api_key={}",
            pl.lit(tmdb_type),
            pl.col("tmdb_id"),
            pl.lit(os.environ["TMDB_API_KEY"]),
        )
        .map(_request_text, return_dtype=pl.Utf8)
        .str.json_extract(dtype=pl.Struct([pl.Field("id", pl.Int64)]))
        .struct.field("id")
        .is_not_null()
        .alias("exists")
    )


FIND_RESPONSE_DTYPE = pl.Struct(
    {
        "movie_results": pl.List(pl.Struct({"id": pl.Int64})),
        "tv_results": pl.List(pl.Struct({"id": pl.Int64})),
        "person_results": pl.List(pl.Struct({"id": pl.Int64})),
    }
)


def tmdb_find(tmdb_type: TMDB_TYPE, external_id_type: str) -> pl.Expr:
    return (
        pl.format(
            "https://api.themoviedb.org/3/find/{}?api_key={}&external_source={}",
            pl.col(external_id_type),
            pl.lit(os.environ["TMDB_API_KEY"]),
            pl.lit(external_id_type),
        )
        .map(_request_text, return_dtype=pl.Utf8)
        .str.json_extract(dtype=FIND_RESPONSE_DTYPE)
        .struct.field(f"{tmdb_type}_results")
        .arr.first()
        .struct.field("id")
        .cast(pl.UInt32)
        .alias("tmdb_id")
    )


# Internal

OUTDATED = pl.col("date") >= pl.col("retrieved_at").dt.round("1d")
NEVER_FETCHED = pl.col("retrieved_at").is_null()
MISSING_STATUS = pl.col("success").is_null()
DUPLICATE_IMDB_IDS = (
    pl.col("imdb_numeric_id").is_not_null() & pl.col("imdb_numeric_id").is_duplicated()
)


def _tmdb_outdated_external_ids(
    latest_changes_df: pl.LazyFrame,
    external_ids_df: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        latest_changes_df.join(external_ids_df, on="id", how="left")
        .sort(pl.col("retrieved_at"), descending=True)
        .filter(OUTDATED | NEVER_FETCHED | MISSING_STATUS | DUPLICATE_IMDB_IDS)
        .head(10_000)
        .select(["id"])
    )


def _insert_tmdb_external_ids(
    df: pl.LazyFrame,
    tmdb_type: TMDB_TYPE,
    tmdb_ids: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        pl.concat([df, fetch_tmdb_external_ids(tmdb_ids, tmdb_type)])
        .unique(subset=["id"], keep="last")
        .pipe(align_to_index, name="id")
    )


def _request_text(urls: pl.Series) -> pl.Series:
    @backoff.on_exception(backoff.expo, requests.exceptions.ReadTimeout, max_tries=3)
    def get_text(url: str) -> str:
        return session.get(url, timeout=3).text

    return series_apply_with_tqdm(
        urls, get_text, return_dtype=pl.Utf8, desc="Fetching URLs"
    )


def main_changes(tmdb_type: TMDB_TYPE) -> None:
    update_ipc(
        "latest_changes.arrow",
        lambda df: insert_tmdb_latest_changes(df, tmdb_type),
    )


def main_external_ids(tmdb_type: TMDB_TYPE):
    latest_changes_df = pl.scan_ipc("latest_changes.arrow")
    external_ids_df = pl.scan_ipc("external_ids.arrow")

    tmdb_ids = _tmdb_outdated_external_ids(
        latest_changes_df=latest_changes_df,
        external_ids_df=external_ids_df,
    )

    external_ids_df = _insert_tmdb_external_ids(
        external_ids_df,
        tmdb_type=tmdb_type,
        tmdb_ids=tmdb_ids,
    ).collect()

    external_ids_df.write_ipc("external_ids.arrow", compression="lz4")
