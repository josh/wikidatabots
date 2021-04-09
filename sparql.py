import os

import requests

url = "https://query.wikidata.org/sparql"
session = requests.Session()

session.headers.update({"Accept": "application/sparql-results+json"})

if "WIKIDATA_USER_AGENT" in os.environ:
    session.headers.update({"User-Agent": os.environ["WIKIDATA_USER_AGENT"]})


def sparql(query):
    r = session.get(url, params={"query": query})
    r.raise_for_status()
    return list(parse_results(r.json()))


def parse_results(resp):
    vars = resp["head"]["vars"]
    bindings = resp["results"]["bindings"]

    for binding in bindings:
        result = {}
        for var in vars:
            result[var] = parse_value(binding.get(var))
        yield result


def parse_value(obj):
    if not obj or not obj.get("type"):
        return obj
    elif obj["type"] == "literal":
        return obj["value"]
    elif obj["type"] == "uri":
        return obj["value"]
    else:
        return obj


if __name__ == "__main__":
    import json
    import sys

    query = sys.stdin.readlines()
    result = sparql(query)
    json.dump(result, sys.stdout, indent=2)
