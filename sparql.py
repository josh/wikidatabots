"""
Small API wrapper for interacting with Wikidata's SPARQL query service.
<https://query.wikidata.org/>
"""

import json
import logging
import math
import os
import platform

import backoff
import requests

url = "https://query.wikidata.org/sparql"
session = requests.Session()

session.headers.update({"Accept": "application/sparql-results+json"})

USER_AGENT = []

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


@backoff.on_exception(backoff.expo, TimeoutException, max_tries=6)
@backoff.on_exception(backoff.expo, json.decoder.JSONDecodeError, max_tries=3)
def sparql(query):
    """
    Execute SPARQL query on Wikidata. Returns simplified results array.
    """

    r = session.post(url, data={"query": query})

    if r.status_code == 500 and "java.util.concurrent.TimeoutException" in r.text:
        raise TimeoutException(query)

    r.raise_for_status()

    data = r.json()
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

    def format_value(obj):
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
        else:
            return obj

    return list(results())


def fetch_statements(qids, properties):
    query = "SELECT ?statement ?item ?property ?value WHERE { "
    query += values_query(qids)
    query += """
    OPTIONAL {
      ?item ?property ?statement.
      ?statement ?ps ?value.
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
    }
    """
    query += "FILTER(" + " || ".join(["(?ps = ps:" + p + ")" for p in properties]) + ")"
    query += "}"

    items = {}

    for result in sparql(query):
        statement = result["statement"]
        qid = result["item"]
        prop = result["property"]
        value = result["value"]

        item = items[qid] = items.get(qid, {})
        properties = item[prop] = item.get(prop, [])

        properties.append((statement, value))

    return items


def sample_items(property, limit, type=None):
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

    items = set()
    for result in sparql(query):
        assert result["item"]
        items.add(result["item"])
    return items


def values_query(qids, binding="item"):
    values = " ".join("wd:{}".format(qid) for qid in qids)
    return "VALUES ?" + binding + " { " + values + " }"


if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO)

    query = sys.stdin.readlines()
    result = sparql(query)
    json.dump(result, sys.stdout, indent=2)
