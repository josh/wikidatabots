"""
Small API wrapper for interacting with Wikidata's SPARQL query service.
<https://query.wikidata.org/>
"""

import os

import requests

url = "https://query.wikidata.org/sparql"
session = requests.Session()

session.headers.update({"Accept": "application/sparql-results+json"})

if "WIKIDATA_USER_AGENT" in os.environ:
    session.headers.update({"User-Agent": os.environ["WIKIDATA_USER_AGENT"]})


class TimeoutException(Exception):
    pass


def sparql(query):
    """
    Execute SPARQL query on Wikidata. Returns simplified results array.
    """

    r = session.get(url, params={"query": query})

    if r.status_code == 500 and "java.util.concurrent.TimeoutException" in r.text:
        raise TimeoutException()

    r.raise_for_status()

    data = r.json()
    vars = data["head"]["vars"]
    bindings = data["results"]["bindings"]

    def results():
        for binding in bindings:
            result = {}
            for var in vars:
                if var in binding:
                    result[var] = format_value(binding[var])
                else:
                    result[var] = None
            yield result

    def format_value(obj):
        if obj["type"] == "literal":
            return obj["value"]
        elif obj["type"] == "uri":
            if obj["value"].startswith("http://www.wikidata.org/entity/"):
                return obj["value"].replace("http://www.wikidata.org/entity/", "")
            else:
                return obj["value"]
        else:
            return obj

    return list(results())


if __name__ == "__main__":
    import json
    import sys

    query = sys.stdin.readlines()
    result = sparql(query)
    json.dump(result, sys.stdout, indent=2)
