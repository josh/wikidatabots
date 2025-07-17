from functools import cache

import polars as pl

from polars_requests import prepare_request, request, response_text

_BLOCKED_PAGE_ID = 103442925

_QUERY_DTYPE = pl.Struct(
    {
        "query": pl.Struct(
            {
                "pages": pl.Struct(
                    {
                        "103442925": pl.Struct({"extract": pl.Utf8}),
                    }
                )
            }
        )
    }
)


@cache
def _blocked_qids() -> pl.Series:
    return (
        pl.DataFrame({"pageids": [_BLOCKED_PAGE_ID]})
        .select(
            prepare_request(
                url="https://www.wikidata.org/w/api.php",
                fields={
                    "action": "query",
                    "format": "json",
                    "pageids": pl.col("pageids"),
                    "prop": "extracts",
                    "explaintext": "1",
                },
            )
            .pipe(
                request,
                log_group="wikidata",
                retry_count=3,
                bad_statuses={429},
                min_time=60,
            )
            .pipe(response_text)
            .str.json_decode(_QUERY_DTYPE)
            .struct.field("query")
            .struct.field("pages")
            .struct.field("103442925")
            .struct.field("extract")
            .str.extract_all(r"(Q[0-9]+)")
            .alias("qid"),
        )
        .explode("qid")
        .sort("qid")
        .to_series()
    )


# MARK: pl.Expr.map_batches
_BLOCKED_EXPR = pl.lit(None).map_batches(
    lambda s: _blocked_qids(), return_dtype=pl.Utf8
)


def is_blocked_item(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(r"(Q[0-9]+)").is_in(_BLOCKED_EXPR.implode())
