"""
Wikidata page modifications wrapper using pywikibot.

MUST be logged in first. See pwb.py
"""

import logging
import re

import requests

from constants import QID


def page_text(page_title: str) -> str | None:
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


def page_qids(page_title: str, blocked: bool = False) -> set[QID]:
    qids: set[QID] = set()

    text = page_text(page_title)
    if not text:
        logging.warn(f"page: {page_title} not found")
        return qids

    for m in re.findall(r"(Q[0-9]+)", text):
        qids.add(QID(m))

    if not blocked:
        qids = qids - blocked_qids()

    logging.debug(f"page: {page_title} {len(qids)} results")

    return qids


_blocked_qids = None


def blocked_qids() -> set[QID]:
    global _blocked_qids
    if not _blocked_qids:
        _blocked_qids = page_qids("User:Josh404Bot/Blocklist", blocked=True)
    return _blocked_qids
