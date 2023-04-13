# pyright: strict

from functools import partial

import polars as pl

from polars_requests import Session, prepare_request, response_text, urllib3_requests
from polars_utils import expr_indicies_sorted, groups_of

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
    read_timeout=10.0,
    retry_count=2,
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
            .pipe(urllib3_requests, session=_SESSION, log_group="itunes_lookup")
            .pipe(response_text)
            .str.json_extract(_RESULTS_DTYPE)
            .struct.field("results")
            .arr.eval(pl.element().pipe(_lookup_result))
            .flatten()
            .sort()
            .alias("result")
        )
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
