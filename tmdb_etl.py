import datetime
import os
import sys
from typing import Literal

import polars as pl

from polars_requests import prepare_request, request, response_date, response_text
from polars_utils import (
    align_to_index,
    gzip_decompress,
    lazy_map_reduce_batches,
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


def _cast_tmdb_type(tmdb_type: str) -> TMDB_TYPE:
    if tmdb_type == "movie":
        return "movie"
    elif tmdb_type == "tv":
        return "tv"
    elif tmdb_type == "person":
        return "person"
    else:
        raise ValueError(f"Invalid TMDB type: {tmdb_type}")


_TMDB_EXTERNAL_SOURCES: set[_TMDB_EXTERNAL_SOURCE] = {
    "imdb_id",
    "tvdb_id",
    "wikidata_id",
}


def _cast_tmdb_external_source(external_source: str) -> _TMDB_EXTERNAL_SOURCE:
    if external_source == "imdb_id":
        return "imdb_id"
    elif external_source == "tvdb_id":
        return "tvdb_id"
    elif external_source == "wikidata_id":
        return "wikidata_id"
    else:
        raise ValueError(f"Invalid TMDB external source: {external_source}")


def extract_imdb_numeric_id(expr: pl.Expr, tmdb_type: TMDB_TYPE) -> pl.Expr:
    return (
        expr.str.extract(_IMDB_ID_PATTERN[tmdb_type], 1)
        .cast(pl.UInt32)
        .alias("imdb_numeric_id")
    )


def _extract_wikidata_numeric_id(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(r"Q(\d+)", 1).cast(pl.UInt32).alias("wikidata_numeric_id")


def tmdb_external_ids(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    assert df.collect_schema()["id"] == pl.UInt32
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
            .str.json_decode(dtype=_EXTERNAL_IDS_RESPONSE_DTYPE)
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


def tmdb_changes(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    assert df.collect_schema() == {"date": pl.Date}
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
            .str.json_decode(dtype=_CHANGES_RESPONSE_DTYPE)
            .struct.field("results")
            .list.reverse()
            .alias("results")
        )
        .explode("results")
        .unnest("results")
        .select("id", "date", "adult")
        .drop_nulls(subset=["id"])
        .unique(subset=["id"], keep="last", maintain_order=True)
    )


def insert_tmdb_latest_changes(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    def map_function(df: pl.LazyFrame) -> pl.LazyFrame:
        dates_df = df.select(
            pl.date_range(
                pl.col("date").max().dt.offset_by("-1d").alias("start_date"),
                datetime.date.today(),
                interval="1d",
                eager=False,
            ).alias("date")
        )
        return tmdb_changes(dates_df, tmdb_type=tmdb_type)

    def reduce_function(df: pl.DataFrame, df_new: pl.DataFrame) -> pl.DataFrame:
        return df.pipe(update_or_append, df_new, on="id").pipe(
            align_to_index, name="id"
        )

    return df.pipe(
        lazy_map_reduce_batches,
        map_function=map_function,
        reduce_function=reduce_function,
    )


_EXISTS_TMDB_TYPE = Literal["movie", "tv", "person", "collection"]


def tmdb_exists(expr: pl.Expr, tmdb_type: _EXISTS_TMDB_TYPE) -> pl.Expr:
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
        .str.json_decode(dtype=pl.Struct([pl.Field("id", pl.UInt32)]))
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
        external_id_type = _cast_tmdb_external_source(output_name)

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
        .str.json_decode(dtype=_FIND_RESPONSE_DTYPE)
        .struct.field(f"{tmdb_type}_results")
        .list.first()
        .struct.field("id")
        .alias("tmdb_id")
    )


_CHANGED = pl.col("date") >= pl.col("retrieved_at").dt.round("1d")
_NEVER_FETCHED = pl.col("retrieved_at").is_null()
_OLDEST_METADATA = pl.col("retrieved_at").rank("ordinal") <= 1_000


def insert_tmdb_external_ids(df: pl.LazyFrame, tmdb_type: TMDB_TYPE) -> pl.LazyFrame:
    def map_function(df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.filter(_CHANGED | _NEVER_FETCHED | _OLDEST_METADATA)
            .select("id")
            .pipe(tmdb_external_ids, tmdb_type=tmdb_type)
        )

    def reduce_function(df: pl.DataFrame, df_new: pl.DataFrame) -> pl.DataFrame:
        return df.pipe(update_or_append, df_new, on="id").pipe(
            align_to_index, name="id"
        )

    return df.pipe(
        lazy_map_reduce_batches,
        map_function=map_function,
        reduce_function=reduce_function,
    )


def _export_date() -> datetime.date:
    now = datetime.datetime.now(datetime.timezone.utc)
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
                bad_statuses={403},
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
            .str.json_decode(
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
        .join(export_df, on="id", how="left", coalesce=True)
        .with_columns(pl.col("in_export").fill_null(False))
        .select(_COLUMNS)
    )


def _log_retrieved_at(df: pl.DataFrame) -> pl.DataFrame:
    retrieved_at = df.select(pl.col("retrieved_at").min()).item()
    print(f"Oldest retrieved_at: {retrieved_at}", file=sys.stderr)
    return df


def _main() -> None:
    pl.enable_string_cache()

    tmdb_type = _cast_tmdb_type(sys.argv[1])

    def _update(df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.pipe(insert_tmdb_latest_changes, tmdb_type)
            .pipe(_insert_tmdb_export_flag, tmdb_type)
            .pipe(insert_tmdb_external_ids, tmdb_type)
            # MARK: pl.Expr.map_batches
            .map_batches(_log_retrieved_at)
        )

    update_parquet("tmdb.parquet", _update, key="id")


if __name__ == "__main__":
    _main()
