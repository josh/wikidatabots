# pyright: strict, reportUnknownMemberType=false, reportUnknownVariableType=false

"""
Small API wrapper for interacting with Wikidata's SPARQL query service.
<https://query.wikidata.org/>
"""

import json
import logging
import math
import os
import platform
import time
from collections.abc import Iterable
from threading import Lock
from typing import Any, Literal, TypedDict

import polars as pl
import rdflib
from rdflib import URIRef

from actions import warn
from polars_requests import Session, prepare_request, urllib3_requests
from polars_utils import apply_with_tqdm
from wikidata import PID, QID

_LOCK = Lock()
_WIKIDATA_SPARQL_SESSION = Session(
    read_timeout=90,
    retry_statuses={500},
    retry_count=10,
    retry_allowed_methods=["GET", "POST"],
    retry_raise_on_status=True,
)


_USER_AGENT_PARTS: list[str] = []

if "WIKIDATA_USERNAME" in os.environ:
    _USER_AGENT_PARTS.append(
        "{username}/1.0 (User:{username})".format(
            username=os.environ["WIKIDATA_USERNAME"]
        )
    )
else:
    warn("WIKIDATA_USERNAME unset")

_USER_AGENT_PARTS.append(f"Python/{platform.python_version()}")
_USER_AGENT_STR = " ".join(_USER_AGENT_PARTS)


class SPARQLHead(TypedDict):
    vars: list[str]


class SPARQLIRIResult(TypedDict):
    type: Literal["uri"]
    value: str


SPARQLLiteralValue = object


class SPARQLLiteralResult(TypedDict):
    type: Literal["literal"]
    value: SPARQLLiteralValue


class SPARQLLiteralWithDatatypeResult(TypedDict):
    type: Literal["literal"]
    value: SPARQLLiteralValue
    # datatype: str


class SPARQLBlankNodeResult(TypedDict):
    type: Literal["bnode"]


SPARQLResult = (
    SPARQLIRIResult
    | SPARQLLiteralResult
    | SPARQLLiteralWithDatatypeResult
    | SPARQLBlankNodeResult
)


class SPARQLResults(TypedDict):
    bindings: list[dict[str, SPARQLResult]]


class SPARQLDocument(TypedDict):
    head: SPARQLHead
    results: SPARQLResults


class SlowQueryWarning(Warning):
    pass


class SPARQLQueryError(Exception):
    pass


def sparql(query: str) -> list[Any]:
    """
    Execute SPARQL query on Wikidata. Returns simplified results array.
    """

    with _LOCK:
        start = time.time()
        http = _WIKIDATA_SPARQL_SESSION.poolmanager()
        r = http.request(
            "POST",
            "https://query.wikidata.org/sparql",
            fields={"query": query},
            headers={
                "Accept": "application/sparql-results+json",
                "User-Agent": _USER_AGENT_STR,
            },
            encode_multipart=False,
        )
        duration = time.time() - start

    response_data = r.data
    assert isinstance(response_data, bytes)

    if r.status != 200:
        raise SPARQLQueryError(f"Query errored with status {r.status}:\n{query}")

    data: SPARQLDocument = json.loads(response_data)
    vars = data["head"]["vars"]
    bindings = data["results"]["bindings"]

    result_count = len(bindings)
    logging.info(f"sparql: {result_count:,} results in {duration:,.2f}s")

    def results():
        for binding in bindings:
            yield {var: format_value(binding.get(var)) for var in vars}

    def format_value(obj: SPARQLResult | None):
        if obj is None:
            return None
        elif obj["type"] == "literal":
            return obj["value"]
        elif obj["type"] == "uri":
            if obj["value"].startswith("http://www.wikidata.org/entity/Q"):
                return obj["value"][31:]
            elif obj["value"].startswith("http://www.wikidata.org/prop/P"):
                return obj["value"][29:]
            elif obj["value"] == "http://wikiba.se/ontology#DeprecatedRank":
                return "deprecated"
            elif obj["value"] == "http://wikiba.se/ontology#NormalRank":
                return "normal"
            elif obj["value"] == "http://wikiba.se/ontology#PreferredRank":
                return "preferred"
            else:
                return URIRef(obj["value"])
        elif obj["type"] == "bnode":
            return None
        else:
            return None

    return list(results())


def _sparql_csv(query: str, _stacklevel: int = 0) -> bytes:
    with _LOCK:
        start = time.time()
        http = _WIKIDATA_SPARQL_SESSION.poolmanager()
        r = http.request(
            "POST",
            "https://query.wikidata.org/sparql",
            fields={"query": query},
            headers={"Accept": "text/csv", "User-Agent": _USER_AGENT_STR},
            encode_multipart=False,
        )
        duration = time.time() - start

    if r.status != 200:
        raise SPARQLQueryError(f"Query errored with status {r.status}:\n{query}")

    if duration > 45:
        logging.warn(f"sparql: {duration:,.2f}s")
        warn(query, SlowQueryWarning, stacklevel=2 + _stacklevel)
    elif duration > 5:
        logging.info(f"sparql: {duration:,.2f}s")
    else:
        logging.debug(f"sparql: {duration:,.2f}s")

    return r.data


def sparql_df(
    query: str,
    columns: list[str] | None = None,
    schema: dict[str, pl.PolarsDataType] | None = None,
) -> pl.LazyFrame:
    if columns and not schema:
        schema = {column: pl.Utf8 for column in columns}
    assert schema, "missing schema"

    def sparql_df_inner(df: pl.DataFrame) -> pl.DataFrame:
        return pl.read_csv(_sparql_csv(query, _stacklevel=2), dtypes=schema)

    return pl.LazyFrame().map(sparql_df_inner, schema=schema)


def fetch_statements(
    qids: Iterable[QID],
    properties: Iterable[PID],
    deprecated: bool = False,
) -> dict[QID, dict[PID, list[tuple[URIRef, str]]]]:
    query = "SELECT ?statement ?item ?property ?value WHERE { "
    query += values_query(qids)
    query += """
    OPTIONAL {
      ?item ?property ?statement.
      ?statement ?ps ?value.
      ?statement wikibase:rank ?rank.
    """
    if deprecated:
        query += "  FILTER(?rank = wikibase:DeprecatedRank)"
    else:
        query += "  FILTER(?rank != wikibase:DeprecatedRank)"
    query += "}"
    query += "FILTER(" + " || ".join(["(?ps = ps:" + p + ")" for p in properties]) + ")"
    query += "}"

    Result = TypedDict(
        "Result", {"statement": URIRef, "item": QID, "property": PID, "value": str}
    )
    results: list[Result] = sparql(query)

    items: dict[QID, dict[PID, list[tuple[URIRef, str]]]] = {}
    for result in results:
        statement = result["statement"]
        qid = result["item"]
        prop = result["property"]
        value = result["value"]

        item = items[qid] = items.get(qid, {})
        properties2 = item[prop] = item.get(prop, [])

        properties2.append((statement, value))

    return items


_STATEMENTS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "statement": pl.Utf8,
    "item": pl.Utf8,
    "property": pl.Utf8,
    "value": pl.Utf8,
}


SampleType = Literal["created", "updated", "random"]


def sample_items(
    property: PID,
    limit: int,
    type: SampleType | None = None,
) -> set[QID]:
    if type is None:
        items: set[QID] = set()
        items |= sample_items(property, type="created", limit=math.floor(limit / 3))
        items |= sample_items(property, type="updated", limit=math.floor(limit / 3))
        items |= sample_items(property, type="random", limit=limit - len(items))
        return items

    elif type == "random":
        query = """
        SELECT ?item WHERE {
          SERVICE bd:sample {
            ?item wdt:?property [].
            bd:serviceParam bd:sample.limit ?limit ;
              bd:sample.sampleType "RANDOM".
          }
        }
        """
    elif type == "created":
        query = """
        SELECT ?item {
          SERVICE wikibase:mwapi {
            bd:serviceParam wikibase:endpoint "www.wikidata.org";
                            wikibase:api "Generator" ;
                            wikibase:limit "once" ;
                            mwapi:generator "search";
                            mwapi:gsrsearch "haswbstatement:?property" ;
                            mwapi:gsrsort "create_timestamp_desc" ;
                            mwapi:gsrlimit "?limit".
            ?item wikibase:apiOutputItem mwapi:title.
          }
        }
        """
    elif type == "updated":
        query = """
        SELECT ?item {
          SERVICE wikibase:mwapi {
            bd:serviceParam wikibase:endpoint "www.wikidata.org";
                            wikibase:api "Generator" ;
                            wikibase:limit "once" ;
                            mwapi:generator "search";
                            mwapi:gsrsearch "haswbstatement:?property" ;
                            mwapi:gsrsort "last_edit_desc" ;
                            mwapi:gsrlimit "?limit".
            ?item wikibase:apiOutputItem mwapi:title.
          }
        }
        """
    else:
        assert False, "unknown type"

    query = query.replace("?property", property)
    query = query.replace("?limit", str(limit))

    Result = TypedDict("Result", {"item": QID})
    results: list[Result] = sparql(query)

    return set([result["item"] for result in results])


def values_query(qids: Iterable[QID], binding: str = "item") -> str:
    values = " ".join(f"wd:{qid}" for qid in qids)
    return "VALUES ?" + binding + " { " + values + " }"


_HYDRA = rdflib.Namespace("http://www.w3.org/ns/hydra/core#")
_WD = rdflib.Namespace("http://www.wikidata.org/")
_RDF = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")

_WIKIDATA_LDF_SESSION = Session()


def fetch_property_statements(pid: str) -> pl.LazyFrame:
    predicate = f"http://www.wikidata.org/prop/statement/{pid}"

    page_count_expr = (
        prepare_request(
            "https://query.wikidata.org/bigdata/ldf",
            fields={"predicate": predicate},
            headers={"Accept": "application/n-triples"},
        )
        .pipe(
            urllib3_requests, session=_WIKIDATA_LDF_SESSION, log_group="wikidata_query"
        )
        .struct.field("data")
        .pipe(
            apply_with_tqdm,
            _parse_page_count,
            return_dtype=pl.Int64,
            log_group="parse_rdf_triples",
        )
        .cast(pl.UInt32)
    )

    page_expr = pl.arange(1, page_count_expr + 1, dtype=pl.UInt32)
    assert isinstance(page_expr, pl.Expr)

    return (
        pl.LazyFrame()
        .select(
            prepare_request(
                pl.lit("https://query.wikidata.org/bigdata/ldf"),
                fields={"predicate": predicate, "page": page_expr},
                headers={"Accept": "application/n-triples"},
            )
            .pipe(
                urllib3_requests,
                session=_WIKIDATA_LDF_SESSION,
                log_group="wikidata_query",
            )
            .struct.field("data")
            .pipe(
                apply_with_tqdm,
                _parse_page,
                return_dtype=pl.List(
                    pl.Struct(
                        [pl.Field("subject", pl.Utf8), pl.Field("object", pl.Utf8)]
                    )
                ),
                log_group="parse_rdf_triples",
            )
            .explode()
            .alias("triple")
        )
        .unnest("triple")
    )


def _parse_page_count(data: bytes) -> int:
    graph = rdflib.Graph()
    graph.parse(data=data)

    page_collection_uri = next(
        graph.subjects(
            predicate=_RDF.type,
            object=_HYDRA.PagedCollection,
        )
    )
    assert isinstance(page_collection_uri, URIRef)

    total_items_obj = graph.value(
        subject=page_collection_uri,
        predicate=_HYDRA.totalItems,
    )
    assert isinstance(total_items_obj, rdflib.Literal)
    total_items = total_items_obj.value
    assert isinstance(total_items, int)

    items_per_page_obj = graph.value(
        subject=page_collection_uri,
        predicate=_HYDRA.itemsPerPage,
    )
    assert isinstance(items_per_page_obj, rdflib.Literal)
    items_per_page = items_per_page_obj.value
    assert isinstance(items_per_page, int)

    return math.ceil(total_items / items_per_page)


def _parse_page(data: bytes) -> list[dict[str, str]]:
    graph = rdflib.Graph()
    graph.parse(data=data)
    results: list[dict[str, str]] = []
    for subject, predicate, object in graph:
        if isinstance(predicate, str) and predicate.startswith(_WD):
            results.append({"subject": str(subject), "object": str(object)})
    return results


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    query = sys.stdin.read()
    result = sparql(query)
    json.dump(result, sys.stdout, indent=2)
