# pyright: strict

import datetime
import os
from typing import Literal

import polars as pl

from polars_requests import Session, response_date, response_text, urllib3_request_urls
from polars_utils import align_to_index, update_ipc

TMDB_TYPE = Literal["movie", "tv", "person"]

CHANGES_SCHEMA = {
    "id": pl.UInt32,
    "has_changes": pl.Boolean,
    "date": pl.Date,
    "adult": pl.Boolean,
}

EXTERNAL_IDS_SCHEMA = {
    "id": pl.UInt32,
    "success": pl.Boolean,
    "retrieved_at": pl.Datetime(time_unit="ns"),
    "imdb_numeric_id": pl.UInt32,
    "tvdb_id": pl.UInt32,
    "wikidata_numeric_id": pl.UInt32,
}

_SESSION = Session(
    ok_statuses={200, 404},
    connect_timeout=1.0,
    read_timeout=3.0,
    retry_count=3,
    retry_backoff_factor=1.0,
)

_IMDB_ID_PATTERN: dict[TMDB_TYPE, str] = {
    "movie": r"tt(\d+)",
    "tv": r"tt(\d+)",
    "person": r"nm(\d+)",
}

_EXTERNAL_IDS_RESPONSE_DTYPE = pl.Struct(
    {
        "success": pl.Boolean,
        "id": pl.Int64,
        "imdb_id": pl.Utf8,
        "tvdb_id": pl.UInt32,
        "wikidata_id": pl.Utf8,
    }
)

_CHANGES_RESPONSE_DTYPE = pl.Struct(
    {"results": pl.List(pl.Struct({"id": pl.Int64, "adult": pl.Boolean}))}
)

_FIND_RESPONSE_DTYPE = pl.Struct(
    {
        "movie_results": pl.List(pl.Struct({"id": pl.Int64})),
        "tv_results": pl.List(pl.Struct({"id": pl.Int64})),
        "person_results": pl.List(pl.Struct({"id": pl.Int64})),
    }
)


def tmdb_external_ids(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    assert df.schema["id"] == pl.UInt32
    return (
        df.with_columns(
            pl.format(
                "https://api.themoviedb.org/3/{}/{}/external_ids?api_key={}",
                pl.lit(tmdb_type),
                pl.col("id"),
                pl.lit(os.environ["TMDB_API_KEY"]),
            )
            .pipe(urllib3_request_urls, session=_SESSION)
            .alias("response")
        )
        .with_columns(
            pl.col("response")
            .pipe(response_text)
            .str.json_extract(dtype=_EXTERNAL_IDS_RESPONSE_DTYPE)
            .alias("data")
        )
        .with_columns(
            pl.col("id"),
            pl.col("data").struct.field("success").fill_null(True).alias("success"),
            (
                pl.col("response")
                .pipe(response_date)
                .cast(pl.Datetime(time_unit="ns"))
                .alias("retrieved_at")
            ),
            (
                pl.col("data")
                .struct.field("imdb_id")
                .pipe(extract_imdb_numeric_id, tmdb_type)
            ),
            pl.col("data").struct.field("tvdb_id").alias("tvdb_id"),
            (
                pl.col("data")
                .struct.field("wikidata_id")
                .pipe(_extract_wikidata_numeric_id)
            ),
        )
        .drop(["response", "data"])
    )


def extract_imdb_numeric_id(expr: pl.Expr, tmdb_type: TMDB_TYPE) -> pl.Expr:
    return (
        expr.str.extract(_IMDB_ID_PATTERN[tmdb_type], 1)
        .cast(pl.UInt32)
        .alias("imdb_numeric_id")
    )


def _extract_wikidata_numeric_id(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(r"Q(\d+)", 1).cast(pl.UInt32).alias("wikidata_numeric_id")


def insert_tmdb_latest_changes(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    assert df.schema == CHANGES_SCHEMA

    df = df.cache()
    dates_df = df.select(
        pl.date_range(
            low=pl.col("date").max().dt.offset_by("-1d").alias("start_date"),
            high=datetime.date.today(),
            interval="1d",
        ).alias("date")
    )

    return (
        pl.concat([df, tmdb_changes(dates_df, tmdb_type=tmdb_type)])
        .unique(subset="id", keep="last")
        .pipe(align_to_index, name="id")
        .with_columns(pl.col("has_changes").fill_null(False))
    )


def tmdb_changes(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    assert df.schema == {"date": pl.Date}
    return (
        df.with_columns(
            pl.format(
                "https://api.themoviedb.org/3/{}/changes"
                "?api_key={}&start_date={}&end_date={}",
                pl.lit(tmdb_type),
                pl.lit(os.environ["TMDB_API_KEY"]),
                pl.col("date"),
                (pl.col("date").dt.offset_by("1d")),
            ).alias("url")
        )
        .select(
            [
                pl.col("date"),
                pl.col("url")
                .pipe(urllib3_request_urls, session=_SESSION)
                .pipe(response_text)
                .str.json_extract(dtype=_CHANGES_RESPONSE_DTYPE)
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
        .pipe(urllib3_request_urls, session=_SESSION)
        .pipe(response_text)
        .str.json_extract(dtype=pl.Struct([pl.Field("id", pl.Int64)]))
        .struct.field("id")
        .is_not_null()
        .alias("exists")
    )


def tmdb_find(tmdb_type: TMDB_TYPE, external_id_type: str) -> pl.Expr:
    return (
        pl.format(
            "https://api.themoviedb.org/3/find/{}?api_key={}&external_source={}",
            pl.col(external_id_type),
            pl.lit(os.environ["TMDB_API_KEY"]),
            pl.lit(external_id_type),
        )
        .pipe(urllib3_request_urls, session=_SESSION)
        .pipe(response_text)
        .str.json_extract(dtype=_FIND_RESPONSE_DTYPE)
        .struct.field(f"{tmdb_type}_results")
        .arr.first()
        .struct.field("id")
        .cast(pl.UInt32)
        .alias("tmdb_id")
    )


_OUTDATED = pl.col("date") >= pl.col("retrieved_at").dt.round("1d")
_NEVER_FETCHED = pl.col("retrieved_at").is_null()
_MISSING_STATUS = pl.col("success").is_null()
_DUPLICATE_IMDB_IDS = (
    pl.col("imdb_numeric_id").is_not_null() & pl.col("imdb_numeric_id").is_duplicated()
)


def _tmdb_outdated_external_ids(
    latest_changes_df: pl.LazyFrame,
    external_ids_df: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        latest_changes_df.join(external_ids_df, on="id", how="left")
        .sort(pl.col("retrieved_at"), descending=True)
        .filter(_OUTDATED | _NEVER_FETCHED | _MISSING_STATUS | _DUPLICATE_IMDB_IDS)
        .head(10_000)
        .select(["id"])
    )


def _insert_tmdb_external_ids(
    df: pl.LazyFrame,
    tmdb_type: TMDB_TYPE,
    tmdb_ids: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        pl.concat([df, tmdb_external_ids(tmdb_ids, tmdb_type)])
        .unique(subset=["id"], keep="last")
        .pipe(align_to_index, name="id")
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
