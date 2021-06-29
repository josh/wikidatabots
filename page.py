"""
Wikidata page modifications wrapper using pywikibot.

MUST be logged in first. See pwb.py
"""

import re
import sys

import requests


def edit(title, text, username, summary=None):
    """
    Edit an existing wiki page.
    """

    import pywikibot

    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    site = pywikibot.Site("wikidata", "wikidata")
    page = pywikibot.Page(site, title)
    page.text = text
    page.save(summary)
    return page


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


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Create and edit Wikidata pages.")
    parser.add_argument("--username", action="store")
    parser.add_argument("--title", action="store")
    parser.add_argument("--summary", action="store")
    parser.add_argument("cmd", action="store")
    args = parser.parse_args()

    if args.cmd == "edit":
        edit(
            username=args.username or os.environ["WIKIDATA_USERNAME"],
            title=args.title,
            text=sys.stdin.read(),
            summary=args.summary,
        )
    else:
        parser.print_usage()
