# pyright: strict


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
            .pipe(request, log_group="wikidata")
            .pipe(response_text)
            .str.json_extract(_QUERY_DTYPE)
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


def blocklist() -> set[str]:
    return set(_blocked_qids())
