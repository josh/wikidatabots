"""
Batch Wikidata statements deprecator.

MUST be logged in first. See pwb.py
"""

import csv
from typing import Any, TextIO

import pywikibot
import pywikibot.config
from pywikibot import DataSite, ItemPage

from properties import (
    REASON_FOR_DEPRECATED_RANK_PID,
    REASON_FOR_DEPRECATED_RANK_PROPERTY,
)
from wikidata import SITE


def process_batch(username: str, csv_file: TextIO):
    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    repo: DataSite = SITE.data_repository()

    for (statement, reason) in csv.reader(csv_file):
        process_statement(repo, statement, reason)


def process_statement(repo: DataSite, statement: str, reason: str):
    (item, claim) = find_claim(repo, statement)
    assert item
    assert claim

    reason_item: ItemPage = pywikibot.ItemPage(repo, reason)
    assert reason_item

    claim.setRank("deprecated")

    if not claim.qualifiers.get(REASON_FOR_DEPRECATED_RANK_PID):
        qualifier = REASON_FOR_DEPRECATED_RANK_PROPERTY.newClaim()
        qualifier.isQualifier = True
        qualifier.setTarget(reason_item)
        claim.qualifiers[REASON_FOR_DEPRECATED_RANK_PID] = [qualifier]

    item.editEntity({"claims": [claim.toJSON()]})


def find_claim(repo: DataSite, guid: str):
    assert "$" in guid
    qid = guid.split("$", 1)[0]
    item = pywikibot.ItemPage(repo, qid)
    properties: Any = item.get()["claims"]

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
