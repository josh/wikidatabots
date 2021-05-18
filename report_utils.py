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


def sample_qids(property, count=1000, username=WIKIDATA_USERNAME):
    limit = math.floor(count / 3)

    qids = set()
    qids |= sparql.sample_items(property, type="random", limit=limit)
    qids |= sparql.sample_items(property, type="created", limit=limit)
    qids |= sparql.sample_items(property, type="updated", limit=limit)

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
        return pages[pageid]["extract"]


def page_qids(page_title):
    qids = set()

    text = page_text(page_title)
    if text:
        for m in re.findall(r"(Q[0-9]+)", text):
            qids.add(m)

    return qids
