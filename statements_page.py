"""
Wikidata page modifications wrapper using pywikibot.

MUST be logged in first. See pwb.py
"""

import csv
import urllib

import page
import wikitext
from utils import uniq


def edit_statements_page(title, csv_file, username, summary=None):
    """
    Edit a wiki page of suggested statements.
    """

    rows = csv.reader(csv_file)
    (header, property) = next(rows)
    assert header == "qid"

    statements = []
    for (qid, value) in rows:
        statements.append((qid, property, value))
    statements.sort()
    statements = list(uniq(statements))

    lines = []
    for (entity, property, value) in statements:
        lines.append(
            "* {{Statement|" + entity + "|" + property + "|" + str(value) + "}}"
        )

    if statements:
        lines.append("")
        lines.append(
            wikitext.link("Add via QuickStatements", quickstatements_url(statements))
        )

    lines.append("")
    text = "\n".join(lines)

    return page.edit(title, text, username, summary)


def quickstatements_url(commands):
    hash = urllib.parse.urlencode({"v1": "||".join(["|".join(c) for c in commands])})  # type: ignore
    return "https://quickstatements.toolforge.org/#{}".format(hash)


if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(
        description="Create and edit wiki page of statements."
    )
    parser.add_argument("--username", action="store")
    parser.add_argument("--title", action="store")
    parser.add_argument("--summary", action="store")
    args = parser.parse_args()

    edit_statements_page(
        username=args.username or os.environ["WIKIDATA_USERNAME"],
        title=args.title,
        csv_file=sys.stdin,
        summary=args.summary,
    )
