# pyright: strict

import datetime
import logging
from functools import partial
from typing import Literal

import polars as pl

from polars_requests import (
    Session,
    prepare_request,
    response_header_value,
    response_text,
    urllib3_requests,
)
from polars_utils import (
    assert_expression,
    expr_indicies_sorted,
    groups_of,
    limit,
    update_or_append,
    update_parquet,
)
from sparql import sparql_df

_COUNTRY = Literal[
    "au",
    "au",
    "br",
    "ca",
    "cn",
    "de",
    "dk",
    "es",
    "fr",
    "gb",
    "ie",
    "in",
    "it",
    "jp",
    "mx",
    "nl",
    "nz",
    "pl",
    "pr",
    "sw",
    "tw",
    "us",
    "vg",
    "vi",
]

_COUNTRIES: set[_COUNTRY] = {
    "au",
    "br",
    "ca",
    "cn",
    "de",
    "es",
    "fr",
    "gb",
    "in",
    "it",
    "jp",
    "us",
}

_LOOKUP_BATCH_SIZE = 150


def lookup_itunes_id(
    expr: pl.Expr, country: str, batch_size: int = _LOOKUP_BATCH_SIZE
) -> pl.Expr:
    return expr.map(
        partial(_lookup_itunes_id, country=country, batch_size=batch_size),
        return_dtype=_LOOKUP_DTYPE,
    )


_RESULT_DTYPE = pl.Struct(
    {
        "wrapperType": pl.Utf8,
        "artistType": pl.Utf8,
        "artistId": pl.UInt64,
        "artistName": pl.Utf8,
        "collectionType": pl.Utf8,
        "collectionId": pl.UInt64,
        "collectionName": pl.Utf8,
        "trackId": pl.UInt64,
        "trackName": pl.Utf8,
        "kind": pl.Utf8,
    }
)

_RESULTS_DTYPE = pl.Struct({"results": pl.List(_RESULT_DTYPE)})

_LOOKUP_DTYPE = pl.Struct(
    [
        pl.Field("id", pl.UInt64),
        pl.Field("type", pl.Utf8),
        pl.Field("name", pl.Utf8),
        pl.Field("kind", pl.Utf8),
    ]
)

_SESSION = Session(
    connect_timeout=1.0,
    read_timeout=20.0,
    retry_count=3,
    retry_statuses={413, 429, 500, 503},
)


def _lookup_itunes_id(s: pl.Series, country: str, batch_size: int) -> pl.Series:
    return (
        s.to_frame("id")
        .lazy()
        .select(
            pl.lit("https://itunes.apple.com/lookup")
            .pipe(
                prepare_request,
                fields={
                    "id": pl.col("id")
                    .pipe(groups_of, n=batch_size)
                    .cast(pl.List(pl.Utf8))
                    .arr.join(separator=","),
                    "country": country,
                },
            )
            .pipe(
                urllib3_requests, session=_SESSION, log_group=f"itunes_lookup_{country}"
            )
            .pipe(response_text)
            .str.json_extract(_RESULTS_DTYPE)
            .struct.field("results")
            .alias("result")
        )
        .explode("result")
        .select(
            pl.col("result").pipe(_lookup_result).alias("result"),
        )
        .sort("result")
        .select(
            pl.col("result").take(  # type: ignore
                expr_indicies_sorted(
                    pl.lit(s),
                    pl.col("result").struct.field("id"),
                )
            )
        )
        .collect()
        .to_series()
    )


def _lookup_result(expr: pl.Expr) -> pl.Expr:
    return (
        pl.when(expr.struct.field("wrapperType") == "collection")
        .then(
            pl.struct(
                expr.struct.field("collectionId").alias("id"),
                expr.struct.field("collectionType").alias("type"),
                expr.struct.field("collectionName").alias("name"),
                pl.lit(None).alias("kind"),
            )
        )
        .when(expr.struct.field("wrapperType") == "artist")
        .then(
            pl.struct(
                expr.struct.field("artistId").alias("id"),
                expr.struct.field("artistType").alias("type"),
                expr.struct.field("artistName").alias("name"),
                pl.lit(None).alias("kind"),
            )
        )
        .otherwise(
            pl.struct(
                expr.struct.field("trackId").alias("id"),
                pl.lit(None).alias("type"),
                expr.struct.field("trackName").alias("name"),
                expr.struct.field("kind").alias("kind"),
            )
        )
    )


def _timestamp() -> pl.Expr:
    return pl.lit(datetime.datetime.now()).dt.round("1s").dt.cast_time_unit("ms")


def fetch_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
    country_results: list[pl.Expr] = []
    country_types: list[pl.Expr] = []
    country_kinds: list[pl.Expr] = []
    country_available_flags: list[pl.Expr] = []
    any_country_available_flags: list[pl.Expr] = []

    for country in _COUNTRIES:
        result_colname = f"{country}_result"
        country_colname = f"{country}_country"
        result_col = pl.col(result_colname)

        country_results.append(
            pl.col("id").pipe(lookup_itunes_id, country=country).alias(result_colname)
        )
        country_types.append(result_col.struct.field("type"))
        country_kinds.append(result_col.struct.field("kind"))
        country_available_flags.append(
            result_col.struct.field("id").is_not_null().alias(country_colname)
        )
        any_country_available_flags.append(pl.col(country_colname))

    return (
        df.select(
            pl.col("id"),
            _timestamp().alias("retrieved_at"),
            *country_results,
        )
        .select(
            pl.col("id"),
            pl.col("retrieved_at"),
            pl.coalesce(*country_types).cast(pl.Categorical).alias("type"),
            pl.coalesce(*country_kinds).cast(pl.Categorical).alias("kind"),
            *country_available_flags,
        )
        .with_columns(
            pl.Expr.or_(*any_country_available_flags).alias("any_country"),
        )
    )


def itunes_legacy_kind(type: pl.Expr, kind: pl.Expr) -> pl.Expr:
    return (
        pl.when(type == "Album")
        .then("album")
        .when(type == "TV Show")
        .then("tv-show")
        .when(type == "TV Season")
        .then("tv-season")
        .when(type == "Movie Bundle")
        .then("movie-collection")
        .when(kind == "ebook")
        .then("book")
        .when(kind == "feature-movie")
        .then("movie")
        .when(kind == "software")
        .then("app")
        .when(kind == "podcast")
        .then("podcast")
        .otherwise(None)
    )


def itunes_legacy_view_url(
    id: pl.Expr, type: pl.Expr, kind: pl.Expr, region: str = "us"
) -> pl.Expr:
    return (
        pl.when(itunes_legacy_kind(type=type, kind=kind).is_not_null())
        .then(
            pl.format(
                "https://itunes.apple.com/{}/{}/id{}",
                pl.lit(region),
                itunes_legacy_kind(type=type, kind=kind),
                id,
            )
        )
        .otherwise(None)
    )


_ITUNES_REDIRECT_SESSION = Session(follow_redirects=False, ok_statuses={200, 301, 404})


def appletv_redirect_url(id: pl.Expr, type: pl.Expr, kind: pl.Expr) -> pl.Expr:
    return (
        itunes_legacy_view_url(id=id, type=type, kind=kind)
        .pipe(prepare_request)
        .pipe(
            urllib3_requests,
            session=_ITUNES_REDIRECT_SESSION,
            log_group="itunes.apple.com",
        )
        .pipe(response_header_value, name="Location")
        .map(_mask_urls, return_dtype=pl.Utf8)
    )


# TODO: Extract this pattern into utils
def _mask_urls(s: pl.Series) -> pl.Series:
    return (
        pl.DataFrame({"url": s})
        .select(
            pl.when(pl.col("url").str.starts_with("https://tv.apple.com/"))
            .then(pl.col("url"))
            .otherwise(None)
        )
        .to_series()
    )


_ITUNES_PROPERTY_ID = Literal[
    "P2281",
    "P2850",
    "P3861",
    "P5260",
    "P5655",
    "P5842",
    "P6250",
    "P6381",
    "P6395",
    "P6398",
    "P6998",
]

ITUNES_PROPERTY_IDS: set[_ITUNES_PROPERTY_ID] = {
    "P2281",
    "P2850",
    "P3861",
    "P5260",
    "P5655",
    "P5842",
    "P6250",
    "P6381",
    "P6395",
    "P6398",
    "P6998",
}

_QUERY = """
SELECT DISTINCT ?id WHERE {
  _:b0 ps:P0000 ?id.
  FILTER(xsd:integer(?id))
}
"""


def _wikidata_itunes_ids(pid: _ITUNES_PROPERTY_ID) -> pl.LazyFrame:
    return (
        sparql_df(_QUERY.replace("P0000", pid), schema={"id": pl.UInt64})
        .pipe(assert_expression, pl.col("id").is_unique())
        .pipe(assert_expression, pl.col("id").is_not_null())
    )


def wikidata_itunes_all_ids() -> pl.LazyFrame:
    return pl.concat(
        [_wikidata_itunes_ids(pid) for pid in ITUNES_PROPERTY_IDS],
        parallel=False,
    ).unique("id")


def _discover_ids(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.pipe(update_or_append, wikidata_itunes_all_ids(), on="id")
        .sort("id")
        .unique("id")
    )


_OLDEST_METADATA = pl.col("retrieved_at").rank("ordinal") < (1 * _LOOKUP_BATCH_SIZE)
_MISSING_METADATA = pl.col("retrieved_at").is_null()


def _backfill_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.cache()
    df_updated = df.filter(_MISSING_METADATA | _OLDEST_METADATA).pipe(fetch_metadata)
    return df.pipe(update_or_append, df_updated, on="id").sort("id")


_REDIRECT_CHECK_LIMIT = 10


def _backfill_appletv_redirect_url(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.cache()

    df_updated = (
        df.select("id", "type", "kind", "appletv_redirect_url")
        .filter(
            (pl.col("kind") == "feature-movie")
            & pl.col("appletv_redirect_url").is_null()
        )
        .pipe(
            limit, soft=_REDIRECT_CHECK_LIMIT, desc="missing appletv_redirect_url frame"
        )
        .with_columns(
            appletv_redirect_url(
                id=pl.col("id"),
                type=pl.col("type"),
                kind=pl.col("kind"),
            ).alias("appletv_redirect_url")
        )
    )

    return df.pipe(update_or_append, df_updated, on="id").sort("id")


def main() -> None:
    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.pipe(_discover_ids)
            .pipe(_backfill_metadata)
            .pipe(_backfill_appletv_redirect_url)
        )

    with pl.StringCache():
        update_parquet("itunes.parquet", update, key="id")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
