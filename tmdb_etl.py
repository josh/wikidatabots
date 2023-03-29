# pyright: strict

import datetime
import gzip
import logging
import os
import random
import sys
from typing import Literal

import polars as pl

from polars_requests import (
    Session,
    prepare_request,
    response_date,
    response_text,
    urllib3_requests,
)
from polars_utils import (
    align_to_index,
    assert_expression,
    outlier_exprs,
    update_ipc,
    update_or_append,
)

TMDB_TYPE = Literal["movie", "tv", "person"]
_TMDB_EXTERNAL_SOURCE = Literal["imdb_id", "tvdb_id", "wikidata_id"]

SCHEMA = {
    "id": pl.UInt32,
    "date": pl.Date,
    "adult": pl.Boolean,
    "in_export": pl.Boolean,
    "success": pl.Boolean,
    "retrieved_at": pl.Datetime(time_unit="ns"),
    "imdb_numeric_id": pl.UInt32,
    "tvdb_id": pl.UInt32,
    "wikidata_numeric_id": pl.UInt32,
}

_COLUMNS = [
    "id",
    "date",
    "adult",
    "in_export",
    "success",
    "retrieved_at",
    "imdb_numeric_id",
    "tvdb_id",
    "wikidata_numeric_id",
]

CHANGES_SCHEMA = {
    "id": pl.UInt32,
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
    fields={"api_key": os.environ["TMDB_API_KEY"]},
)

_IMDB_ID_PATTERN: dict[TMDB_TYPE, str] = {
    "movie": r"tt(\d+)",
    "tv": r"tt(\d+)",
    "person": r"nm(\d+)",
}

_EXTERNAL_IDS_RESPONSE_DTYPE = pl.Struct(
    {
        "success": pl.Boolean,
        "id": pl.UInt32,
        "imdb_id": pl.Utf8,
        "tvdb_id": pl.UInt32,
        "wikidata_id": pl.Utf8,
    }
)

_CHANGES_RESPONSE_DTYPE = pl.Struct(
    {"results": pl.List(pl.Struct({"id": pl.UInt32, "adult": pl.Boolean}))}
)

_FIND_RESPONSE_DTYPE = pl.Struct(
    {
        "movie_results": pl.List(pl.Struct({"id": pl.UInt32})),
        "tv_results": pl.List(pl.Struct({"id": pl.UInt32})),
        "person_results": pl.List(pl.Struct({"id": pl.UInt32})),
    }
)

_TMDB_TYPES: set[TMDB_TYPE] = {"movie", "tv", "person"}

_TMDB_EXTERNAL_SOURCES: set[_TMDB_EXTERNAL_SOURCE] = {
    "imdb_id",
    "tvdb_id",
    "wikidata_id",
}


def tmdb_external_ids(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    assert df.schema["id"] == pl.UInt32
    return (
        df.with_columns(
            pl.format(
                "https://api.themoviedb.org/3/{}/{}/external_ids",
                pl.lit(tmdb_type),
                pl.col("id"),
            )
            .pipe(prepare_request)
            .pipe(
                urllib3_requests,
                session=_SESSION,
                log_group=f"api.themoviedb.org/3/{tmdb_type}/external_ids",
            )
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
    assert df.schema == SCHEMA
    df = df.cache()

    dates_df = df.select(
        pl.date_range(
            low=pl.col("date").max().dt.offset_by("-1d").alias("start_date"),
            high=datetime.date.today(),
            interval="1d",
        ).alias("date")
    ).pipe(assert_expression, pl.count() < 30, "Too many dates")

    return df.pipe(
        update_or_append, tmdb_changes(dates_df, tmdb_type=tmdb_type), on="id"
    ).pipe(align_to_index, name="id")


def tmdb_changes(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    assert df.schema == {"date": pl.Date}
    return (
        df.with_columns(
            pl.format("https://api.themoviedb.org/3/{}/changes", pl.lit(tmdb_type))
            .pipe(
                prepare_request,
                fields={
                    "start_date": pl.col("date"),
                    "end_date": pl.col("date").dt.offset_by("1d"),
                },
            )
            .pipe(
                urllib3_requests,
                session=_SESSION,
                log_group=f"api.themoviedb.org/3/{tmdb_type}/changes",
            )
            .pipe(response_text)
            .str.json_extract(dtype=_CHANGES_RESPONSE_DTYPE)
            .struct.field("results")
            .arr.reverse()
            .alias("results")
        )
        .explode("results")
        .select(
            pl.col("results").struct.field("id").alias("id"),
            pl.col("date"),
            pl.col("results").struct.field("adult").alias("adult"),
        )
        .drop_nulls(subset=["id"])
        .unique(subset=["id"], keep="last")
    )


def tmdb_exists(expr: pl.Expr, tmdb_type: TMDB_TYPE) -> pl.Expr:
    return (
        pl.format("https://api.themoviedb.org/3/{}/{}", pl.lit(tmdb_type), expr)
        .pipe(prepare_request)
        .pipe(
            urllib3_requests,
            session=_SESSION,
            log_group=f"api.themoviedb.org/3/{tmdb_type}",
        )
        .pipe(response_text)
        .str.json_extract(dtype=pl.Struct([pl.Field("id", pl.UInt32)]))
        .struct.field("id")
        .is_not_null()
        .alias("exists")
    )


def tmdb_find(
    expr: pl.Expr,
    tmdb_type: TMDB_TYPE,
    external_id_type: _TMDB_EXTERNAL_SOURCE | None = None,
) -> pl.Expr:
    if not external_id_type:
        output_name = expr.meta.output_name()
        assert output_name in _TMDB_EXTERNAL_SOURCES
        external_id_type = output_name

    return (
        pl.format("https://api.themoviedb.org/3/find/{}", expr)
        .pipe(prepare_request, fields={"external_source": external_id_type})
        .pipe(
            urllib3_requests,
            session=_SESSION,
            log_group="api.themoviedb.org/3/find",
        )
        .pipe(response_text)
        .str.json_extract(dtype=_FIND_RESPONSE_DTYPE)
        .struct.field(f"{tmdb_type}_results")
        .arr.first()
        .struct.field("id")
        .alias("tmdb_id")
    )


_TWO_WEEKS_AGO = datetime.date.today() - datetime.timedelta(weeks=2)


def _outlier_expr(df: pl.DataFrame) -> pl.Expr:
    exprs = df.pipe(
        outlier_exprs,
        [
            pl.col("date"),
            pl.col("adult"),
            pl.col("in_export"),
            pl.col("success"),
            pl.col("retrieved_at") < _TWO_WEEKS_AGO,
            pl.col("imdb_numeric_id"),
            pl.col("tvdb_id"),
            pl.col("wikidata_numeric_id"),
        ],
        rmax=3,
        max_count=1000,
    )

    expr_str, expr, count = random.choice(exprs)
    logging.info(f"Refreshing {count:,} outlier rows against `{expr_str}`")

    return expr


_CHANGED = pl.col("date") >= pl.col("retrieved_at").dt.round("1d")
_NEVER_FETCHED = pl.col("retrieved_at").is_null()
_OUTDATED = _CHANGED | _NEVER_FETCHED
_OUTDATED_LIMIT = 10_000


def insert_tmdb_external_ids(
    df: pl.LazyFrame,
    tmdb_type: TMDB_TYPE,
    outdated_expr: pl.Expr = _OUTDATED,
) -> pl.LazyFrame:
    assert df.schema == SCHEMA
    df = df.cache()

    new_external_ids_df = (
        df.filter(outdated_expr)
        .pipe(assert_expression, pl.count() < _OUTDATED_LIMIT, "Too many outdated rows")
        .select("id")
        .pipe(tmdb_external_ids, tmdb_type=tmdb_type)
    )

    return df.pipe(update_or_append, new_external_ids_df, on="id").pipe(
        align_to_index, name="id"
    )


def _decompress(data: bytes) -> str:
    return gzip.decompress(data).decode("utf-8")


def _export_date() -> datetime.date:
    now = datetime.datetime.utcnow()
    if now.hour >= 8:
        return now.date()
    else:
        return (now - datetime.timedelta(days=1)).date()


_EXPORT_TYPE = Literal["movie", "tv_series", "person", "collection"]


def _tmdb_export(types: list[_EXPORT_TYPE], date: datetime.date) -> pl.LazyFrame:
    return (
        pl.LazyFrame({"type": types}, schema={"type": pl.Categorical})
        .with_columns(pl.lit(date).alias("date"))
        .select(
            pl.col("type"),
            pl.format(
                "http://files.tmdb.org/p/exports/{}_ids_{}.json.gz",
                pl.col("type"),
                pl.col("date").dt.strftime("%m_%d_%Y"),
            )
            .pipe(prepare_request)
            .pipe(
                urllib3_requests,
                session=_SESSION,
                log_group="files.tmdb.org/p/exports",
            )
            .struct.field("data")
            .apply(_decompress)
            .str.split("\n")
            .alias("lines"),
        )
        .explode("lines")
        .filter(pl.col("lines").str.starts_with("{"))
        .select(
            pl.col("type"),
            pl.col("lines")
            .str.json_extract(
                dtype=pl.Struct(
                    {
                        "adult": pl.Boolean,
                        "id": pl.UInt32,
                        "original_title": pl.Utf8,
                        "popularity": pl.Float64,
                        "video": pl.Boolean,
                    }
                )
            )
            .alias("item"),
        )
        .select(
            pl.col("item").struct.field("id").alias("id"),
            pl.col("type"),
            pl.col("item").struct.field("adult").alias("adult"),
            pl.col("item").struct.field("original_title").alias("original_title"),
            pl.col("item").struct.field("popularity").alias("popularity"),
            pl.col("item").struct.field("video").alias("video"),
        )
        .sort(by="id")
        .pipe(assert_expression, pl.col("id").is_unique())
    )


def tmdb_export(
    tmdb_type: TMDB_TYPE,
    date: datetime.date = _export_date(),
) -> pl.LazyFrame:
    if tmdb_type == "movie":
        return _tmdb_export(types=["movie", "collection"], date=date)
    elif tmdb_type == "tv":
        return _tmdb_export(types=["tv_series"], date=date)
    elif tmdb_type == "person":
        return _tmdb_export(types=["person"], date=date)


def _insert_tmdb_export_flag(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    assert df.schema == SCHEMA

    export_df = tmdb_export(tmdb_type).select(
        pl.col("id"),
        pl.lit(True).alias("in_export"),
    )

    return (
        df.drop("in_export")
        .join(export_df, on="id", how="left")
        .with_columns(pl.col("in_export").fill_null(False))
        .select(_COLUMNS)
    )


def main() -> None:
    tmdb_type = sys.argv[1]
    assert tmdb_type in _TMDB_TYPES

    def _update(df: pl.LazyFrame) -> pl.LazyFrame:
        df = df.cache()
        outdated_expr = _OUTDATED | _outlier_expr(df.collect())

        return (
            df.pipe(insert_tmdb_latest_changes, tmdb_type)
            .pipe(_insert_tmdb_export_flag, tmdb_type)
            .pipe(insert_tmdb_external_ids, tmdb_type, outdated_expr)
        )

    update_ipc("tmdb.arrow", _update)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
