# pyright: strict

import datetime
import logging
import os
import sys
from typing import Literal

import polars as pl

from polars_requests import Session, response_date, response_text, urllib3_request_urls
from polars_utils import align_to_index

TMDB_TYPE = Literal["movie", "tv", "person"]
TMDB_EXTERNAL_SOURCE = Literal["imdb_id", "tvdb_id", "wikidata_id"]

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

_TMDB_EXTERNAL_SOURCES: set[TMDB_EXTERNAL_SOURCE] = {
    "imdb_id",
    "tvdb_id",
    "wikidata_id",
}


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
            )
            .pipe(urllib3_request_urls, session=_SESSION)
            .pipe(response_text)
            .str.json_extract(dtype=_CHANGES_RESPONSE_DTYPE)
            .struct.field("results")
            .arr.reverse()
            .alias("results")
        )
        .explode("results")
        .select(
            pl.col("results").struct.field("id").alias("id"),
            pl.lit(True).alias("has_changes"),
            pl.col("date"),
            pl.col("results").struct.field("adult").alias("adult"),
        )
    )


def tmdb_exists(expr: pl.Expr, tmdb_type: TMDB_TYPE) -> pl.Expr:
    return (
        pl.format(
            "https://api.themoviedb.org/3/{}/{}?api_key={}",
            pl.lit(tmdb_type),
            expr,
            pl.lit(os.environ["TMDB_API_KEY"]),
        )
        .pipe(urllib3_request_urls, session=_SESSION)
        .pipe(response_text)
        .str.json_extract(dtype=pl.Struct([pl.Field("id", pl.UInt32)]))
        .struct.field("id")
        .is_not_null()
        .alias("exists")
    )


def tmdb_find(
    expr: pl.Expr,
    tmdb_type: TMDB_TYPE,
    external_id_type: TMDB_EXTERNAL_SOURCE | None = None,
) -> pl.Expr:
    if not external_id_type:
        output_name = expr.meta.output_name()
        assert output_name in _TMDB_EXTERNAL_SOURCES
        external_id_type = output_name

    return (
        pl.format(
            "https://api.themoviedb.org/3/find/{}?api_key={}&external_source={}",
            expr,
            pl.lit(os.environ["TMDB_API_KEY"]),
            pl.lit(external_id_type),
        )
        .pipe(urllib3_request_urls, session=_SESSION)
        .pipe(response_text)
        .str.json_extract(dtype=_FIND_RESPONSE_DTYPE)
        .struct.field(f"{tmdb_type}_results")
        .arr.first()
        .struct.field("id")
        .alias("tmdb_id")
    )


_OUTDATED = pl.col("date") >= pl.col("retrieved_at").dt.round("1d")
_NEVER_FETCHED = pl.col("retrieved_at").is_null()
_MISSING_STATUS = pl.col("success").is_null()


def _tmdb_outdated_external_ids(
    changes_df: pl.LazyFrame,
    external_ids_df: pl.LazyFrame,
) -> pl.LazyFrame:
    assert changes_df.schema == CHANGES_SCHEMA
    assert external_ids_df.schema == EXTERNAL_IDS_SCHEMA
    return (
        changes_df.join(external_ids_df, on="id", how="left")
        .sort(pl.col("retrieved_at"), descending=True)
        .filter(_OUTDATED | _NEVER_FETCHED | _MISSING_STATUS)
        .head(10_000)
        .select(["id"])
    )


def _insert_tmdb_external_ids(
    df: pl.LazyFrame,
    tmdb_type: TMDB_TYPE,
    tmdb_ids: pl.LazyFrame,
) -> pl.LazyFrame:
    assert df.schema == EXTERNAL_IDS_SCHEMA
    assert tmdb_ids.schema == {"id": pl.UInt32}
    return (
        pl.concat([df, tmdb_external_ids(tmdb_ids, tmdb_type)])
        .unique(subset=["id"], keep="last")
        .pipe(align_to_index, name="id")
    )


def update_changes_and_external_ids(
    changes_df: pl.LazyFrame,
    external_ids_df: pl.LazyFrame,
    tmdb_type: TMDB_TYPE,
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    # TODO: cache() not working here
    changes_df = (
        insert_tmdb_latest_changes(changes_df, tmdb_type=tmdb_type).collect().lazy()
    )
    external_ids_df = external_ids_df.cache()

    outdated_ids = _tmdb_outdated_external_ids(
        changes_df=changes_df,
        external_ids_df=external_ids_df,
    )

    external_ids_df = _insert_tmdb_external_ids(
        external_ids_df,
        tmdb_type=tmdb_type,
        tmdb_ids=outdated_ids,
    )

    return (changes_df, external_ids_df)


def main() -> None:
    tmdb_type = sys.argv[1]
    assert tmdb_type in _TMDB_TYPES

    changes_df, external_ids_df = pl.collect_all(
        update_changes_and_external_ids(
            changes_df=pl.scan_ipc("latest_changes.arrow"),
            external_ids_df=pl.scan_ipc("external_ids.arrow"),
            tmdb_type=tmdb_type,
        )
    )

    changes_df.write_ipc("latest_changes.arrow", compression="lz4")
    external_ids_df.write_ipc("external_ids.arrow", compression="lz4")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
