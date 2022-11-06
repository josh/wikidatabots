"""
Small API wrapper for interacting with Wikidata's SPARQL query service.
<https://query.wikidata.org/>
"""

import builtins
import json
import logging
import math
import os
import platform
from typing import Iterable, Literal, Optional, TypedDict

import backoff
import requests

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

USER_AGENT.append("requests/" + requests.__version__)
USER_AGENT.append("Python/" + platform.python_version())
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
    datatype: str


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


@backoff.on_exception(backoff.expo, TimeoutException, max_tries=14)
@backoff.on_exception(backoff.expo, json.decoder.JSONDecodeError, max_tries=3)
def sparql(query: str) -> list[dict[str, object]]:
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

    def format_value(obj: Optional[SPARQLResult]):
        if obj is None:
            return None
        elif obj["type"] == "literal":
            return obj["value"]
        elif obj["type"] == "uri":
            if obj["value"].startswith("http://www.wikidata.org/prop/"):
                return obj["value"].replace("http://www.wikidata.org/prop/", "")
            elif obj["value"] == "http://wikiba.se/ontology#DeprecatedRank":
                return "deprecated"
            elif obj["value"] == "http://wikiba.se/ontology#NormalRank":
                return "normal"
            elif obj["value"] == "http://wikiba.se/ontology#PreferredRank":
                return "preferred"
            elif obj["value"].startswith("http://www.wikidata.org/entity/"):
                label = obj["value"].replace("http://www.wikidata.org/entity/", "")
                if label.startswith("statement/"):
                    return "$".join(label.replace("statement/", "").split("-", 1))
                else:
                    return label
            else:
                return obj["value"]
        elif obj["type"] == "bnode":
            return None
        else:
            return None

    return list(results())


def fetch_statements(
    qids: Iterable[str],
    properties: Iterable[str],
    deprecated: bool = False,
) -> dict[str, dict[str, list[tuple[str, str]]]]:
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

    items: dict[str, dict[str, list[tuple[str, str]]]] = {}

    for result in sparql(query):
        statement = result["statement"]
        qid = result["item"]
        prop = result["property"]
        value = result["value"]

        assert type(statement) is str
        assert type(qid) is str
        assert type(prop) is str
        assert type(value) is str

        item = items[qid] = items.get(qid, {})
        properties2 = item[prop] = item.get(prop, [])

        properties2.append((statement, value))

    return items


def type_constraints(property: str) -> set[str]:
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
    types: set[str] = set()
    for result in sparql(query):
        subclass = result["subclass"]
        assert type(subclass) is str
        types.add(subclass)
    return types


SampleType = Literal["created", "updated", "random"]


def sample_items(
    property: str,
    limit: int,
    type: Optional[SampleType] = None,
) -> set[str]:
    if type is None:
        items = set()
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

    items: set[str] = set()
    for result in sparql(query):
        qid = result["item"]
        assert builtins.type(qid) is str
        items.add(qid)
    return items


def values_query(qids: Iterable[str], binding: str = "item") -> str:
    values = " ".join(f"wd:{qid}" for qid in qids)
    return "VALUES ?" + binding + " { " + values + " }"


if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO)

    query = sys.stdin.read()
    result = sparql(query)
    json.dump(result, sys.stdout, indent=2)
