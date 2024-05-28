import sys
from warnings import warn

import polars as pl

from polars_requests import prepare_request, request, response_date, response_text
from polars_utils import (
    align_to_index,
    lazy_map_reduce_batches,
    update_or_append,
    update_parquet,
)

_API_RETRY_COUNT = 3
_API_RPS: float = 2 / 3

_LOG_GROUP = "api.opencritic.com"

_BROWSER_HEADERS: dict[str, str | pl.Expr] = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://opencritic.com",
    "Host": "api.opencritic.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    + "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Referer": "https://opencritic.com/",
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
_OPENCRITIC_GAME_SCHEMA = dict(_OPENCRITIC_GAME_DTYPE)
_OPENCRITIC_GAME_FIELDS = [n for n in _OPENCRITIC_GAME_SCHEMA]


def _log_ratelimit(responses: pl.Series) -> None:
    requests_remaining: int | None = None
    for response in responses:
        for header in response["headers"]:
            if header["name"].startswith("X-RateLimit-Requests-Remaining"):
                requests_remaining = int(header["value"])

    if requests_remaining and requests_remaining < 25:
        warn(f"X-RateLimit-Requests-Remaining: {requests_remaining}")


def _tidy_game(s: pl.Series) -> pl.Series:
    _log_ratelimit(s)

    return (
        s.to_frame(name="response")
        .select(
            pl.col("response").pipe(response_date).alias("retrieved_at"),
            pl.col("response")
            .pipe(response_text)
            .str.json_decode(dtype=_OPENCRITIC_GAME_API_DTYPE)
            .alias("data"),
        )
        .unnest("data")
        .with_columns(
            pl.col(pl.Utf8).replace({"": None}),
            pl.col(pl.Int8).replace({-1: None}).cast(pl.UInt8),
            pl.col(pl.Int16).replace({-1: None}).cast(pl.UInt16),
            pl.col(pl.Float32).replace({-1: None}),
        )
        .with_columns(
            pl.col("^*At$").str.strptime(
                pl.Datetime(time_unit="ms"),
                "%+",
            ),
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
        .select(_OPENCRITIC_GAME_FIELDS)
        .to_struct(s.name)
    )


def fetch_opencritic_game(expr: pl.Expr) -> pl.Expr:
    return (
        prepare_request(
            url=pl.format("https://api.opencritic.com/api/game/{}", expr),
            headers=_BROWSER_HEADERS,
        )
        .pipe(
            request,
            log_group=_LOG_GROUP,
            min_time=_API_RPS,
            retry_count=_API_RETRY_COUNT,
            ok_statuses={200, 400},
            bad_statuses={429, 502},
        )
        # MARK: pl.Expr.map_batches
        .map_batches(_tidy_game, return_dtype=_OPENCRITIC_GAME_DTYPE)
    )


_GAME_DTYPE = pl.List(pl.Struct({"id": pl.UInt32}))


def _fetch_recently_reviewed() -> pl.LazyFrame:
    return (
        pl.LazyFrame(
            {
                "url": [
                    "game/reviewed-today",
                    "game/reviewed-this-week",
                    "game/recently-released",
                    "game?time=last90&order=asc&sort=score",
                    "game?time=last90&order=desc&sort=score",
                    "game?time=last90&order=asc&sort=date",
                    "game?time=last90&order=desc&sort=date",
                    "game?time=last90&order=asc&sort=num-reviews",
                    "game?time=last90&order=desc&sort=num-reviews",
                ]
            }
        )
        .select(
            prepare_request(
                pl.format("https://api.opencritic.com/api/{}", pl.col("url")),
                headers=_BROWSER_HEADERS,
            )
            .pipe(
                request,
                log_group=_LOG_GROUP,
                min_time=_API_RPS,
                retry_count=_API_RETRY_COUNT,
                bad_statuses={429, 502},
            )
            .alias("response")
            .pipe(response_text)
            .str.json_decode(_GAME_DTYPE)
            .alias("game"),
        )
        .explode("game")
        .unnest("game")
        .select("id")
        .unique("id")
        .with_columns(pl.lit(True).alias("recently_reviewed"))
    )


_OLDEST_DATA = pl.col("retrieved_at").rank("ordinal") < 50
_MISSING_DATA = pl.col("retrieved_at").is_null()
_RECENTLY_REVIEWED = pl.col("recently_reviewed")


def _refresh_games(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.join(_fetch_recently_reviewed(), on="id", how="full", coalesce=True)
        .filter(_OLDEST_DATA | _MISSING_DATA | _RECENTLY_REVIEWED)
        .select("id")
        .with_columns(
            pl.col("id").pipe(fetch_opencritic_game).alias("game"),
        )
        .unnest("game")
    )


def _log_retrieved_at(df: pl.DataFrame) -> pl.DataFrame:
    retrieved_at = df.select(pl.col("retrieved_at").min()).item()
    print(f"Oldest retrieved_at: {retrieved_at}", file=sys.stderr)
    return df


def _main() -> None:
    pl.enable_string_cache()

    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        def reduce_function(df: pl.DataFrame, df_new: pl.DataFrame) -> pl.DataFrame:
            return (
                df.pipe(update_or_append, df_new, on="id")
                .pipe(align_to_index, name="id")
                .pipe(_log_retrieved_at)
            )

        return df.pipe(
            lazy_map_reduce_batches,
            map_function=_refresh_games,
            reduce_function=reduce_function,
        )

    update_parquet("opencritic.parquet", update, key="id")


if __name__ == "__main__":
    _main()
