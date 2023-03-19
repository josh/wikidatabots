# pyright: strict

"""
Small API wrapper for interacting with Wikidata's SPARQL query service.
<https://query.wikidata.org/>
"""

import json
import logging
import math
import os
import platform
from collections.abc import Iterable
from io import BytesIO
from typing import Any, Literal, TypedDict

import backoff
import polars as pl
import requests
from rdflib import URIRef

import timeout
from wikidata import PID, QID

url = "https://query.wikidata.org/sparql"
session = requests.Session()

session.headers.update({"Accept": "application/sparql-results+json"})

USER_AGENT: list[str] = []

if "WIKIDATA_USERNAME" in os.environ:
    USER_AGENT.append(
        "{username}/1.0 (User:{username})".format(
            username=os.environ["WIKIDATA_USERNAME"]
        )
    )
else:
    logging.warn("WARN: WIKIDATA_USERNAME unset")

USER_AGENT.append(f"requests/{requests.__version__}")
USER_AGENT.append(f"Python/{platform.python_version()}")
session.headers.update({"User-Agent": " ".join(USER_AGENT)})


class TimeoutException(Exception):
    pass


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


@backoff.on_exception(
    backoff.expo,
    TimeoutException,
    max_tries=14,
    max_time=timeout.max_time,
)
@backoff.on_exception(
    backoff.expo,
    json.decoder.JSONDecodeError,
    max_tries=3,
    max_time=timeout.max_time,
)
def sparql(query: str) -> list[Any]:
    """
    Execute SPARQL query on Wikidata. Returns simplified results array.
    """

    r = session.post(url, data={"query": query})

    if r.status_code == 500 and "java.util.concurrent.TimeoutException" in r.text:
        raise TimeoutException(query)

    r.raise_for_status()

    data: SPARQLDocument = r.json()
    vars = data["head"]["vars"]
    bindings = data["results"]["bindings"]

    logging.info(
        "sparql: {} results in {} ms".format(
            len(bindings), math.floor(r.elapsed.total_seconds() * 1000)
        )
    )

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


def sparql_csv(query: str) -> BytesIO:
    r = session.post(url, data={"query": query}, headers={"Accept": "text/csv"})

    if r.status_code == 500 and "java.util.concurrent.TimeoutException" in r.text:
        raise TimeoutException(query)
    r.raise_for_status()

    duration = math.floor(r.elapsed.total_seconds() * 1000)
    logging.info(f"sparql: {duration:,} ms")

    return BytesIO(r.content)


def sparql_df(
    query: str,
    columns: list[str] | None = None,
    schema: dict[str, pl.PolarsDataType] | None = None,
) -> pl.LazyFrame:
    if columns and not schema:
        schema = {column: pl.Utf8 for column in columns}
    assert schema, "missing schema"

    def sparql_df_inner(df: pl.DataFrame) -> pl.DataFrame:
        return pl.read_csv(sparql_csv(query), dtypes=schema)

    return pl.LazyFrame().map(sparql_df_inner, schema=schema)


def extract_qid(name: str = "item") -> pl.Expr:
    return pl.col(name).str.extract(r"^http://www.wikidata.org/entity/(Q\d+)$", 1)


def extract_numeric_qid(name: str = "item") -> pl.Expr:
    return (
        pl.col(name)
        .str.extract(r"^http://www.wikidata.org/entity/Q(\d+)$", 1)
        .cast(pl.UInt32)
    )


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


def type_constraints(property: PID) -> set[QID]:
    query = """
    SELECT DISTINCT ?subclass WHERE {
    """
    query += "  wd:" + property + " p:P2302 ?constraint."
    query += """
      ?constraint ps:P2302 wd:Q21503250.
      ?constraint pq:P2308 ?class.
      ?subclass wdt:P279* ?class.
    }
    """
    Result = TypedDict("Result", {"subclass": QID})
    results: list[Result] = sparql(query)

    classes: set[QID] = set()
    for result in results:
        if result["subclass"].startswith("Q"):
            classes.add(result["subclass"])
    return classes


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


if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO)

    query = sys.stdin.read()
    result = sparql(query)
    json.dump(result, sys.stdout, indent=2)
