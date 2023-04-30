# pyright: strict

import json

import polars as pl

from polars_requests import Session, prepare_request, request, response_text

_SESSION = Session()


def _parse_query_json_data(text: str) -> str | None:
    data = json.loads(text)
    pages = data["query"]["pages"]
    for pageid in pages:
        page = pages[pageid]
        if page.get("extract"):
            return page["extract"]
    return None


def page_qids(page_title: str) -> pl.Series:
    return (
        pl.DataFrame({"title": [page_title]})
        .with_columns(
            prepare_request(
                url="https://www.wikidata.org/w/api.php",
                fields={
                    "action": "query",
                    "format": "json",
                    "titles": pl.col("title"),
                    "prop": "extracts",
                    "explaintext": "1",
                },
            )
            .pipe(request, session=_SESSION, log_group="wikidata_api")
            .pipe(response_text)
            .apply(_parse_query_json_data, return_dtype=pl.Utf8)
            .alias("text")
        )
        .select(
            pl.col("text").str.extract_all(r"(Q[0-9]+)").alias("qid"),
        )
        .explode("qid")
        .sort("qid")
        .to_series()
    )
