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


@backoff.on_exception(backoff.expo, TimeoutException, max_tries=3)
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
            if obj["value"].startswith("http://www.wikidata.org/entity/"):
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


if __name__ == "__main__":
    import json
    import sys

    query = sys.stdin.readlines()
    result = sparql(query)
    json.dump(result, sys.stdout, indent=2)
