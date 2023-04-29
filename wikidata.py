# pyright: strict

import json

import polars as pl
from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager

from polars_requests import Session, prepare_request, response_text, urllib3_requests

P = Namespace("http://www.wikidata.org/prop/")
PQ = Namespace("http://www.wikidata.org/prop/qualifier/")
PR = Namespace("http://www.wikidata.org/prop/reference/")
PS = Namespace("http://www.wikidata.org/prop/statement/")
PSV = Namespace("http://www.wikidata.org/prop/statement/value/")
PROV = Namespace("http://www.w3.org/ns/prov#")
WD = Namespace("http://www.wikidata.org/entity/")
WDREF = Namespace("http://www.wikidata.org/reference/")
WDS = Namespace("http://www.wikidata.org/entity/statement/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")
WDV = Namespace("http://www.wikidata.org/value/")
WDNO = Namespace("http://www.wikidata.org/prop/novalue/")
WIKIBASE = Namespace("http://wikiba.se/ontology#")

WIKIDATABOTS = Namespace("https://github.com/josh/wikidatabots#")

NS_MANAGER = NamespaceManager(Graph())
NS_MANAGER.bind("wikibase", WIKIBASE)
NS_MANAGER.bind("wd", WD)
NS_MANAGER.bind("wds", WDS)
NS_MANAGER.bind("wdv", WDV)
NS_MANAGER.bind("wdref", WDREF)
NS_MANAGER.bind("wdt", WDT)
NS_MANAGER.bind("p", P)
NS_MANAGER.bind("wdno", WDNO)
NS_MANAGER.bind("ps", PS)
NS_MANAGER.bind("psv", PSV)
NS_MANAGER.bind("pq", PQ)
NS_MANAGER.bind("pr", PR)


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
            .pipe(urllib3_requests, session=_SESSION, log_group="wikidata_api")
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
