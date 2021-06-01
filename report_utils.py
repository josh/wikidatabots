import math
import os
import re
import sys

import requests

import sparql

if "WIKIDATA_USERNAME" in os.environ:
    WIKIDATA_USERNAME = os.environ["WIKIDATA_USERNAME"]
else:
    print("WARN: WIKIDATA_USERNAME unset", file=sys.stderr)


def sample_qids(
    property, count=1000, constraint_violations=True, username=WIKIDATA_USERNAME
):
    limit = math.floor(count / 3)

    qids = set()
    qids |= sparql.sample_items(property, type="random", limit=limit)
    qids |= sparql.sample_items(property, type="created", limit=limit)
    qids |= sparql.sample_items(property, type="updated", limit=limit)

    if constraint_violations:
        qids |= page_qids(
            "Wikidata:Database reports/Constraint violations/{}".format(property)
        )

    if username:
        qids |= page_qids("User:{}/Maintenance_reports/{}".format(username, property))

    return qids


def page_text(page_title):
    params = {
        "action": "query",
        "format": "json",
        "titles": page_title,
        "prop": "extracts",
        "explaintext": True,
    }

    r = requests.get(
        "https://www.wikidata.org/w/api.php",
        params=params,
    )
    r.raise_for_status()

    data = r.json()
    pages = data["query"]["pages"]

    for pageid in pages:
        page = pages[pageid]
        if page.get("extract"):
            return page["extract"]
    return None


def page_qids(page_title):
    qids = set()

    text = page_text(page_title)
    if not text:
        print(
            "page: {} not found".format(page_title),
            file=sys.stderr,
        )
        return qids

    for m in re.findall(r"(Q[0-9]+)", text):
        qids.add(m)

    print(
        "page: {} {} results".format(page_title, len(qids)),
        file=sys.stderr,
    )

    return qids


def page_statements(page_title):
    text = page_text(page_title)
    if not text:
        print(
            "page: {} not found".format(page_title),
            file=sys.stderr,
        )
        return []

    return re.findall(r".* \((Q\d+)\) .* \((P\d+)\) \"([^\"]+)\"", text)


def duplicate_values(property):
    query = """
    SELECT ?value ?statement ?rank WHERE {
      {
        SELECT ?value (COUNT(?item) AS ?count) WHERE { ?item ps:?property ?value. }
        GROUP BY ?value
        HAVING (?count > 1 )
      }
      ?statement ps:?property ?value.
      ?statement wikibase:rank ?rank.
    }
    ORDER BY ?value ?statement
    """
    query = query.replace("?property", property)
    results = sparql.sparql(query)

    for result in results:
        yield (result["value"], result["statement"], result["rank"])
