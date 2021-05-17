import re

import requests


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
