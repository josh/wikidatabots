"""
Small API wrapper for interacting with Wikidata's SPARQL query service.
<https://query.wikidata.org/>
"""

import math
import os
import platform
import sys

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
    print("WARN: WIKIDATA_USERNAME unset", file=sys.stderr)

USER_AGENT.append("requests/" + requests.__version__)
USER_AGENT.append("Python/" + platform.python_version())
session.headers.update({"User-Agent": " ".join(USER_AGENT)})


class TimeoutException(Exception):
    pass


@backoff.on_exception(backoff.expo, TimeoutException, max_tries=5)
def sparql(query, quiet=False):
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

    if quiet is False:
        print(
            "sparql: {} results in {} ms".format(
                len(bindings), math.floor(r.elapsed.total_seconds() * 1000)
            ),
            file=sys.stderr,
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


class Statement:
    def __init__(self, id, rank, item, property, value):
        assert item.startswith("Q")
        assert property.startswith("P")

        self.id = id
        self.rank = rank
        self.item = item
        self.property = property
        self.value = value

    def __repr__(self):
        return f'Statement("{self.id}","{self.item}","{self.property}","{self.value}")'

    def __str__(self):
        return f"{self.item}|{self.property}|{self.value}"


class StatementSet:
    def __init__(self, statements):
        self._ids = {}
        for s in statements:
            self._ids[s.id] = s

        self._items = {}
        for s in statements:
            if s.item not in self._items:
                self._items[s.item] = set()
            self._items[s.item].add(s)

        self._properties = {}
        for s in statements:
            if s.property not in self._properties:
                self._properties[s.property] = set()
            self._properties[s.property].add(s)

        self.statements = set(statements)

    def __len__(self):
        return len(self._ids)

    def __getitem__(self, key):
        return self._ids[key]

    def filter_item(self, qid):
        assert qid.startswith("Q")
        return StatementSet(self._items.get(qid, []))

    def filter_property(self, property):
        assert property.startswith("P")
        return StatementSet(self._properties.get(property, []))

    def by_item(self):
        for item in self._items:
            yield (item, StatementSet(self._items[item]))

    def by_property(self):
        for property in self._properties:
            yield (property, StatementSet(self._properties[property]))


def get_claims(qids, properties):
    query = "SELECT ?statement ?rank ?item ?property ?value WHERE { "
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

    statements = set()

    for result in sparql(query):
        statements.add(
            Statement(
                id=result["statement"],
                rank=result["rank"],
                item=result["item"],
                property=result["property"],
                value=result["value"],
            )
        )

    return StatementSet(statements)


def sample_items(property, limit=50, type="random"):
    if type == "random":
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

    query = sys.stdin.readlines()
    result = sparql(query)
    json.dump(result, sys.stdout, indent=2)
