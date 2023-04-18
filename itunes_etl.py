# pyright: strict

import datetime
import logging
from functools import partial
from typing import Literal

import polars as pl

from polars_requests import Session, prepare_request, response_text, urllib3_requests
from polars_utils import (
    assert_expression,
    expr_indicies_sorted,
    groups_of,
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


def check_itunes_id(
    expr: pl.Expr, country: str, batch_size: int = _LOOKUP_BATCH_SIZE
) -> pl.Expr:
    return (
        expr.pipe(lookup_itunes_id, country=country, batch_size=batch_size)
        .struct.field("id")
        .is_not_null()
    )


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
    read_timeout=15.0,
    retry_count=3,
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
    return pl.concat(_wikidata_itunes_ids(pid) for pid in ITUNES_PROPERTY_IDS).unique(
        "id"
    )


def _discover_ids(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.pipe(update_or_append, wikidata_itunes_all_ids(), on="id")
        .sort("id")
        .unique("id")
    )


# _OLDEST_METADATA = pl.col("retrieved_at").rank("ordinal") < (10 * _LOOKUP_BATCH_SIZE)
_MISSING_METADATA = pl.col("retrieved_at").is_null()
_TMP_LIMIT = 50 * _LOOKUP_BATCH_SIZE


def _backfill_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.cache()

    df_updated = df.filter(_MISSING_METADATA).head(_TMP_LIMIT).pipe(fetch_metadata)

    return df.pipe(update_or_append, df_updated, on="id").sort("id")


def main() -> None:
    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        return df.pipe(_discover_ids).pipe(_backfill_metadata)

    with pl.StringCache():
        update_parquet("itunes.parquet", update, key="id")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
