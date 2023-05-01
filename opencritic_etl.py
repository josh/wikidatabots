# pyright: strict

import logging
import os

import polars as pl

from polars_requests import (
    Session,
    prepare_request,
    request,
    response_date,
    response_text,
)
from polars_utils import align_to_index, limit, update_or_append, update_parquet

_SESSION = Session(min_time=1 / 3, retry_count=3, ok_statuses={400})

_LOG_GROUP = "opencritic-api.p.rapidapi.com"

_HEADERS: dict[str, str | pl.Expr] = {
    "X-RapidAPI-Host": "opencritic-api.p.rapidapi.com",
    "X-RapidAPI-Key": os.environ["RAPIDAPI_KEY"],
}


_OPENCRITIC_GAME_API_DTYPE = pl.Struct(
    {
        "id": pl.UInt32,
        "name": pl.Utf8,
        "url": pl.Utf8,
        "tier": pl.Utf8,
        "percentRecommended": pl.Float32,
        "numReviews": pl.Int16,
        "numTopCriticReviews": pl.Int16,
        "medianScore": pl.Int8,
        "topCriticScore": pl.Float32,
        "firstReleaseDate": pl.Utf8,
        "createdAt": pl.Utf8,
        "updatedAt": pl.Utf8,
        "firstReviewDate": pl.Utf8,
        "latestReviewDate": pl.Utf8,
        "tenthReviewDate": pl.Utf8,
        "criticalReviewDate": pl.Utf8,
    }
)


def fetch_opencritic_game(expr: pl.Expr) -> pl.Expr:
    return (
        prepare_request(
            url=pl.format("https://opencritic-api.p.rapidapi.com/game/{}", expr),
            headers=_HEADERS,
        )
        .pipe(request, session=_SESSION, log_group=_LOG_GROUP)
        .map(_tidy_game, return_dtype=_OPENCRITIC_GAME_DTYPE)
    )


_OPENCRITIC_GAME_DTYPE = pl.Struct(
    {
        "name": pl.Utf8,
        "url": pl.Utf8,
        "tier": pl.Categorical,
        "percent_recommended": pl.Float32,
        "num_reviews": pl.UInt16,
        "num_top_critic_reviews": pl.UInt16,
        "median_score": pl.UInt8,
        "top_critic_score": pl.Float32,
        "created_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "updated_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "first_release_date": pl.Date,
        "first_review_date": pl.Date,
        "latest_review_date": pl.Date,
        "tenth_review_date": pl.Date,
        "critical_review_date": pl.Date,
        "retrieved_at": pl.Datetime(time_unit="ms"),
    }
)


def _tidy_game(s: pl.Series) -> pl.Series:
    return (
        s.to_frame(name="response")
        .select(
            pl.col("response").pipe(response_date).alias("retrieved_at"),
            pl.col("response")
            .pipe(response_text)
            .str.json_extract(dtype=_OPENCRITIC_GAME_API_DTYPE)
            .alias("data"),
        )
        .unnest("data")
        .with_columns(
            pl.col(pl.Utf8).map_dict({"": None}, default=pl.first()),
            pl.col(pl.Int8).map_dict({-1: None}, default=pl.first()).cast(pl.UInt8),
            pl.col(pl.Int16).map_dict({-1: None}, default=pl.first()).cast(pl.UInt16),
            pl.col(pl.Float32).map_dict({-1: None}, default=pl.first()),
        )
        .with_columns(
            pl.col("^*At$").str.strptime(pl.Datetime(time_unit="ms"), "%+", utc=True),
            pl.col("^*Date$").str.strptime(pl.Date, "%+"),
        )
        .rename(
            {
                "percentRecommended": "percent_recommended",
                "numReviews": "num_reviews",
                "numTopCriticReviews": "num_top_critic_reviews",
                "medianScore": "median_score",
                "topCriticScore": "top_critic_score",
                "firstReleaseDate": "first_release_date",
                "createdAt": "created_at",
                "updatedAt": "updated_at",
                "firstReviewDate": "first_review_date",
                "latestReviewDate": "latest_review_date",
                "tenthReviewDate": "tenth_review_date",
                "criticalReviewDate": "critical_review_date",
            }
        )
        .with_columns(
            pl.col("tier").cast(pl.Categorical),
        )
        .select([f.name for f in _OPENCRITIC_GAME_DTYPE.fields])
        .to_struct(s.name)
    )


_GAME_DTYPE = pl.List(pl.Struct({"id": pl.UInt32}))


def _fetch_recently_reviewed() -> pl.LazyFrame:
    return (
        pl.LazyFrame(
            {
                "url": [
                    "https://opencritic-api.p.rapidapi.com/game/reviewed-today",
                    "https://opencritic-api.p.rapidapi.com/game/reviewed-this-week",
                    "https://opencritic-api.p.rapidapi.com/game/recently-released",
                ]
            }
        )
        .select(
            prepare_request(pl.col("url"), headers=_HEADERS)
            .pipe(request, session=_SESSION, log_group=_LOG_GROUP)
            .alias("response")
            .pipe(response_text)
            .str.json_extract(_GAME_DTYPE)
            .alias("game"),
        )
        .explode("game")
        .unnest("game")
        .select("id")
        .unique("id")
        .with_columns(pl.lit(True).alias("recently_reviewed"))
    )


_OLDEST_DATA = pl.col("retrieved_at").rank("ordinal") < 250
_MISSING_DATA = pl.col("retrieved_at").is_null()
_RECENTLY_REVIEWED = pl.col("recently_reviewed")


def _refresh_games(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.join(_fetch_recently_reviewed(), on="id")
        .filter(_OLDEST_DATA | _MISSING_DATA | _RECENTLY_REVIEWED)
        .select("id")
        .pipe(limit, soft=250, desc="outdated ids")  # TODO: Remove
        .with_columns(
            pl.col("id").pipe(fetch_opencritic_game).alias("game"),
        )
        .unnest("game")
    )


def _main() -> None:
    pl.enable_string_cache(True)

    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        df = df.cache()
        return df.pipe(update_or_append, _refresh_games(df), on="id").pipe(
            align_to_index, name="id"
        )

    update_parquet("opencritic.parquet", update, key="id")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main()
