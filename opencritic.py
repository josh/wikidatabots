# pyright: strict

import os

import polars as pl

from polars_requests import (
    Session,
    prepare_request,
    request,
    response_header_value,
    response_text,
)

_SAFE_SESSION = Session(ok_statuses=range(100, 600))

_SESSION = Session(retry_count=3)


def opencritic_ratelimits() -> pl.LazyFrame:
    return (
        pl.LazyFrame()
        .select(
            prepare_request(
                url="https://opencritic-api.p.rapidapi.com/score-format",
                headers={
                    "X-RapidAPI-Host": "opencritic-api.p.rapidapi.com",
                    "X-RapidAPI-Key": os.environ["RAPIDAPI_KEY"],
                },
            )
            .pipe(
                request,
                session=_SAFE_SESSION,
                log_group="opencritic-api.p.rapidapi.com",
            )
            .alias("response")
        )
        .select(
            (
                pl.col("response")
                .pipe(response_header_value, name="X-RateLimit-Searches-Limit")
                .cast(pl.UInt32)
                .alias("searches_limit")
            ),
            (
                pl.col("response")
                .pipe(response_header_value, name="X-RateLimit-Searches-Remaining")
                .cast(pl.Int32)
                .clip_min(0)
                .cast(pl.UInt32)
                .alias("searches_remaining")
            ),
            (
                (
                    pl.col("response")
                    .pipe(response_header_value, name="X-RateLimit-Searches-Reset")
                    .cast(pl.UInt32)
                    * 1000
                )
                .cast(pl.Duration(time_unit="ms"))
                .alias("searches_reset")
            ),
            (
                pl.col("response")
                .pipe(response_header_value, name="X-RateLimit-Requests-Limit")
                .cast(pl.UInt32)
                .alias("requests_limit")
            ),
            (
                pl.col("response")
                .pipe(response_header_value, name="X-RateLimit-Requests-Remaining")
                .cast(pl.Int32)
                .clip_min(0)
                .cast(pl.UInt32)
                .alias("requests_remaining")
            ),
            (
                (
                    pl.col("response")
                    .pipe(response_header_value, name="X-RateLimit-Requests-Reset")
                    .cast(pl.UInt32)
                    * 1000
                )
                .cast(pl.Duration(time_unit="ms"))
                .alias("requests_reset")
            ),
        )
    )


_OPENCRITIC_GAME_DTYPE = pl.Struct(
    {
        "id": pl.UInt32,
        "name": pl.Utf8,
        "url": pl.Utf8,
        "reviews_count": pl.UInt32,
        "top_critic_score": pl.Float64,
        "latest_review_date": pl.Date,
    }
)


_OPENCRITIC_GAME_API_DTYPE = pl.Struct(
    {
        "id": pl.UInt32,
        "name": pl.Utf8,
        "url": pl.Utf8,
        "numReviews": pl.UInt32,
        "topCriticScore": pl.Float64,
        "latestReviewDate": pl.Utf8,
    }
)


def fetch_opencritic_game(expr: pl.Expr) -> pl.Expr:
    return (
        prepare_request(
            url=pl.format("https://opencritic-api.p.rapidapi.com/game/{}", expr),
            headers={
                "X-RapidAPI-Host": "opencritic-api.p.rapidapi.com",
                "X-RapidAPI-Key": os.environ["RAPIDAPI_KEY"],
            },
        )
        .pipe(
            request,
            session=_SESSION,
            log_group="opencritic-api.p.rapidapi.com",
        )
        .pipe(response_text)
        .str.json_extract(dtype=_OPENCRITIC_GAME_API_DTYPE)
        .map(_tidy_game, return_dtype=_OPENCRITIC_GAME_DTYPE)
    )


def _tidy_game(s: pl.Series) -> pl.Series:
    return (
        s.to_frame(name=s.name)
        .unnest(s.name)
        .select(
            pl.col("id"),
            pl.col("name"),
            pl.col("url"),
            pl.col("numReviews").alias("reviews_count"),
            (
                pl.when(pl.col("topCriticScore") < 0)
                .then(None)
                .otherwise(pl.col("topCriticScore"))
                .alias("top_critic_score")
            ),
            (
                pl.col("latestReviewDate")
                .str.strptime(pl.Date, "%+")
                .alias("latest_review_date")
            ),
        )
        .to_struct(s.name)
    )
