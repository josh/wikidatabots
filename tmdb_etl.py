# pyright: strict

import datetime
import logging
import os
import sys
from typing import Literal

import polars as pl

from polars_requests import prepare_request, request, response_date, response_text
from polars_utils import (
    align_to_index,
    gzip_decompress,
    update_or_append,
    update_parquet,
)

TMDB_TYPE = Literal["movie", "tv", "person"]
_TMDB_EXTERNAL_SOURCE = Literal["imdb_id", "tvdb_id", "wikidata_id"]

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

_API_RETRY_COUNT = 3

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
            .pipe(prepare_request, fields={"api_key": os.environ["TMDB_API_KEY"]})
            .pipe(
                request,
                log_group=f"api.themoviedb.org/3/{tmdb_type}/external_ids",
                ok_statuses={200, 404},
                retry_count=_API_RETRY_COUNT,
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
    df = df.cache()  # MARK: pl.LazyFrame.cache

    dates_df = df.select(
        pl.date_range(
            pl.col("date").max().dt.offset_by("-1d").alias("start_date"),
            datetime.date.today(),
            interval="1d",
            eager=False,
        ).alias("date")
    )

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
                    "api_key": os.environ["TMDB_API_KEY"],
                },
            )
            .pipe(
                request,
                log_group=f"api.themoviedb.org/3/{tmdb_type}/changes",
                retry_count=_API_RETRY_COUNT,
            )
            .pipe(response_text)
            .str.json_extract(dtype=_CHANGES_RESPONSE_DTYPE)
            .struct.field("results")
            .arr.reverse()
            .alias("results")
        )
        .explode("results")
        .unnest("results")
        .select("id", "date", "adult")
        .drop_nulls(subset=["id"])
        .unique(subset=["id"], keep="last", maintain_order=True)
    )


def tmdb_exists(expr: pl.Expr, tmdb_type: TMDB_TYPE) -> pl.Expr:
    return (
        pl.format("https://api.themoviedb.org/3/{}/{}", pl.lit(tmdb_type), expr)
        .pipe(prepare_request, fields={"api_key": os.environ["TMDB_API_KEY"]})
        .pipe(
            request,
            log_group=f"api.themoviedb.org/3/{tmdb_type}",
            ok_statuses={200, 404},
            retry_count=_API_RETRY_COUNT,
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
        .pipe(
            prepare_request,
            fields={
                "external_source": external_id_type,
                "api_key": os.environ["TMDB_API_KEY"],
            },
        )
        .pipe(
            request,
            log_group="api.themoviedb.org/3/find",
            ok_statuses={200, 404},
            retry_count=_API_RETRY_COUNT,
        )
        .pipe(response_text)
        .str.json_extract(dtype=_FIND_RESPONSE_DTYPE)
        .struct.field(f"{tmdb_type}_results")
        .arr.first()
        .struct.field("id")
        .alias("tmdb_id")
    )


_CHANGED = pl.col("date") >= pl.col("retrieved_at").dt.round("1d")
_NEVER_FETCHED = pl.col("retrieved_at").is_null()


def insert_tmdb_external_ids(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    df = df.cache()  # MARK: pl.LazyFrame.cache

    new_external_ids_df = (
        df.filter(_CHANGED | _NEVER_FETCHED)
        .select("id")
        .pipe(tmdb_external_ids, tmdb_type=tmdb_type)
    )

    return df.pipe(update_or_append, new_external_ids_df, on="id").pipe(
        align_to_index, name="id"
    )


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
                request,
                log_group="files.tmdb.org/p/exports",
                retry_count=_API_RETRY_COUNT,
            )
            .struct.field("data")
            .pipe(gzip_decompress)
            .cast(pl.Utf8)
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
        return (
            df.pipe(insert_tmdb_latest_changes, tmdb_type)
            .pipe(_insert_tmdb_export_flag, tmdb_type)
            .pipe(insert_tmdb_external_ids, tmdb_type)
        )

    update_parquet("tmdb.parquet", _update, key="id")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
