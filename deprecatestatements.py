"""
Batch Wikidata statements deprecator.

MUST be logged in first. See pwb.py
"""

import csv
from typing import Any, TextIO

import pywikibot  # type: ignore
import pywikibot.config  # type: ignore
from pywikibot import DataSite, ItemPage  # type: ignore

REASON_FOR_DEPRECATION = "P2241"


def process_batch(username: str, csv_file: TextIO):
    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    site = pywikibot.Site("wikidata", "wikidata")
    repo: DataSite = site.data_repository()

    for (statement, reason) in csv.reader(csv_file):
        process_statement(repo, statement, reason)


def process_statement(repo: DataSite, statement: str, reason: str):
    (item, claim) = find_claim(repo, statement)
    assert item
    assert claim

    reason_item: ItemPage = pywikibot.ItemPage(repo, reason)
    assert reason_item

    claim.setRank("deprecated")

    if not claim.qualifiers.get(REASON_FOR_DEPRECATION):
        qualifier = pywikibot.Claim(repo, REASON_FOR_DEPRECATION)
        qualifier.isQualifier = True
        qualifier.setTarget(reason_item)  # type: ignore
        claim.qualifiers[REASON_FOR_DEPRECATION] = [qualifier]

    item.editEntity({"claims": [claim.toJSON()]})  # type: ignore


def find_claim(repo: DataSite, guid: str):
    assert "$" in guid
    qid = guid.split("$", 1)[0]
    item = pywikibot.ItemPage(repo, qid)
    properties: Any = item.get()["claims"]  # type: ignore

    for property in properties:
        for claim in properties[property]:
            if guid == claim.snak:
                return (item, claim)

    return (None, None)


if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Process Wikidata batch changes.")
    parser.add_argument("--username", action="store")
    args = parser.parse_args()

    process_batch(
        username=args.username
        or os.environ.get("QUICKSTATEMENTS_USERNAME")
        or os.environ["WIKIDATA_USERNAME"],
        csv_file=sys.stdin,
    )
