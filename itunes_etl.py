# pyright: strict

import logging
from functools import partial
from typing import Literal

import polars as pl

from polars_requests import (
    Session,
    prepare_request,
    response_text,
    urllib3_requests,
    urllib3_resolve_redirects,
)
from polars_utils import (
    assert_expression,
    expr_indicies_sorted,
    groups_of,
    limit,
    now,
    update_or_append,
    update_parquet,
    with_outlier_column,
)
from sparql import sparql_df

_COUNTRY = Literal[
    "af",
    "al",
    "dz",
    "ad",
    "ao",
    "ai",
    "ag",
    "ar",
    "am",
    "au",
    "at",
    "az",
    "bs",
    "bh",
    "bd",
    "bb",
    "by",
    "be",
    "bz",
    "bj",
    "bm",
    "bt",
    "bo",
    "ba",
    "bw",
    "br",
    "bn",
    "bg",
    "bf",
    "cv",
    "kh",
    "cm",
    "ca",
    "ky",
    "cf",
    "td",
    "cl",
    "cn",
    "co",
    "cd",
    "cg",
    "cr",
    "hr",
    "cy",
    "cz",
    "ci",
    "dk",
    "dm",
    "do",
    "ec",
    "eg",
    "sv",
    "ee",
    "sz",
    "et",
    "fj",
    "fi",
    "fr",
    "ga",
    "gm",
    "ge",
    "de",
    "gh",
    "gr",
    "gd",
    "gt",
    "gn",
    "gw",
    "gy",
    "hn",
    "hk",
    "hu",
    "is",
    "in",
    "id",
    "iq",
    "ie",
    "il",
    "it",
    "jm",
    "jp",
    "jo",
    "kz",
    "ke",
    "kr",
    "xk",
    "kw",
    "kg",
    "la",
    "lv",
    "lb",
    "lr",
    "ly",
    "li",
    "lt",
    "lu",
    "mo",
    "mg",
    "mw",
    "my",
    "mv",
    "ml",
    "mt",
    "mr",
    "mu",
    "mx",
    "fm",
    "md",
    "mc",
    "mn",
    "me",
    "ms",
    "ma",
    "mz",
    "mm",
    "na",
    "nr",
    "np",
    "nl",
    "nz",
    "ni",
    "ne",
    "ng",
    "no",
    "om",
    "pk",
    "pw",
    "ps",
    "pa",
    "pg",
    "py",
    "pe",
    "ph",
    "pl",
    "pt",
    "qa",
    "mk",
    "ro",
    "ru",
    "rw",
    "kn",
    "lc",
    "vc",
    "ws",
    "st",
    "sa",
    "sn",
    "rs",
    "sc",
    "sl",
    "sg",
    "sk",
    "si",
    "sb",
    "za",
    "es",
    "lk",
    "sr",
    "se",
    "ch",
    "tw",
    "tj",
    "tz",
    "th",
    "to",
    "tt",
    "tn",
    "tr",
    "tm",
    "tc",
    "ug",
    "ua",
    "ae",
    "gb",
    "us",
    "uy",
    "uz",
    "vu",
    "ve",
    "vn",
    "vg",
    "ye",
    "zm",
    "zw",
]

_COUNTRIES: list[_COUNTRY] = [
    "us",
    "ca",
    "gb",
    "au",
    "br",
    "de",
    "es",
    "fr",
    "it",
    "in",
    "jp",
    "cn",
]

_LOOKUP_BATCH_SIZE = 180


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
        "artistLinkUrl": pl.Utf8,
        "collectionType": pl.Utf8,
        "collectionId": pl.UInt64,
        "collectionName": pl.Utf8,
        "collectionViewUrl": pl.Utf8,
        "trackId": pl.UInt64,
        "trackName": pl.Utf8,
        "trackViewUrl": pl.Utf8,
        "kind": pl.Utf8,
    }
)

_RESULTS_DTYPE = pl.Struct({"results": pl.List(_RESULT_DTYPE)})

_LOOKUP_DTYPE = pl.Struct(
    [
        pl.Field("id", pl.UInt64),
        pl.Field("type", pl.Utf8),
        pl.Field("name", pl.Utf8),
        pl.Field("url", pl.Utf8),
        pl.Field("kind", pl.Utf8),
    ]
)

_SESSION = Session(
    connect_timeout=1.0,
    read_timeout=30.0,
    retry_count=5,
    retry_statuses={429, 500, 503},
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
                expr.struct.field("collectionViewUrl").alias("url"),
                pl.lit(None).alias("kind"),
            )
        )
        .when(expr.struct.field("wrapperType") == "artist")
        .then(
            pl.struct(
                expr.struct.field("artistId").alias("id"),
                expr.struct.field("artistType").alias("type"),
                expr.struct.field("artistName").alias("name"),
                expr.struct.field("artistLinkUrl").alias("url"),
                pl.lit(None).alias("kind"),
            )
        )
        .otherwise(
            pl.struct(
                expr.struct.field("trackId").alias("id"),
                pl.lit(None).alias("type"),
                expr.struct.field("trackName").alias("name"),
                expr.struct.field("trackViewUrl").alias("url"),
                expr.struct.field("kind").alias("kind"),
            )
        )
    )


def fetch_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
    country_results: list[pl.Expr] = []
    country_types: list[pl.Expr] = []
    country_kinds: list[pl.Expr] = []
    country_urls: list[pl.Expr] = []
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
        country_urls.append(result_col.struct.field("url"))
        country_available_flags.append(
            result_col.struct.field("id").is_not_null().alias(country_colname)
        )
        any_country_available_flags.append(pl.col(country_colname))

    return (
        df.select(
            pl.col("id"),
            now().alias("retrieved_at"),
            *country_results,
        )
        .select(
            pl.col("id"),
            pl.col("retrieved_at"),
            pl.coalesce(*country_types).cast(pl.Categorical).alias("type"),
            pl.coalesce(*country_kinds).cast(pl.Categorical).alias("kind"),
            pl.coalesce(*country_urls).cast(pl.Utf8).alias("url"),
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


_OLDEST_METADATA = pl.col("retrieved_at").rank("ordinal") < (10 * _LOOKUP_BATCH_SIZE)
_MISSING_METADATA = pl.col("retrieved_at").is_null()
_OUTLIER = pl.col("is_outlier")


def _backfill_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.cache()
    df_updated = df.filter(_MISSING_METADATA | _OLDEST_METADATA | _OUTLIER).pipe(
        fetch_metadata
    )
    return df.pipe(update_or_append, df_updated, on="id").sort("id")


_REDIRECT_CHECK_LIMIT = 250


def _backfill_redirect_url(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.cache()

    df_updated = (
        df.filter(
            pl.col("url").is_not_null() & pl.col("redirect_url").is_null(),
        )
        .select("id", "url", "redirect_url")
        .pipe(limit, soft=_REDIRECT_CHECK_LIMIT, desc="missing redirect_url frame")
        .with_columns(
            pl.col("url")
            .pipe(
                urllib3_resolve_redirects,
                session=_SESSION,
                log_group="apple.com redirect",
            )
            .alias("redirect_url")
        )
    )

    return df.pipe(update_or_append, df_updated, on="id").sort("id")


def _with_outlier_column(df: pl.LazyFrame) -> pl.LazyFrame:
    return with_outlier_column(
        df,
        [
            pl.col("type"),
            pl.col("kind"),
            (pl.col("kind") == "feature-movie").alias("type_movie"),
            pl.col("url"),
            pl.col("redirect_url"),
            pl.col("any_country"),
            pl.col("us_country"),
            # *[pl.col(f"{c}_country") for c in _COUNTRIES],
        ],
        max_count=1_000,
    )


_COLUMN_ORDER: list[str] = [
    "id",
    "retrieved_at",
    "type",
    "kind",
    "url",
    "redirect_url",
    "any_country",
    *[f"{c}_country" for c in _COUNTRIES],
]


def main() -> None:
    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.select(_COLUMN_ORDER)
            .pipe(_discover_ids)
            .pipe(_with_outlier_column)
            .pipe(_backfill_metadata)
            .pipe(_backfill_redirect_url)
            .drop("is_outlier")
            .select(_COLUMN_ORDER)
        )

    with pl.StringCache():
        update_parquet("itunes.parquet", update, key="id")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
