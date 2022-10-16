"""
Batch Wikidata statements deprecator.

MUST be logged in first. See pwb.py
"""

import csv
from typing import TextIO

import pywikibot
import pywikibot.config
from pywikibot import ItemPage

from properties import (
    REASON_FOR_DEPRECATED_RANK_PID,
    REASON_FOR_DEPRECATED_RANK_PROPERTY,
)
from wikidata import SITE, find_claim_by_guid


def process_batch(username: str, csv_file: TextIO):
    pywikibot.config.usernames["wikidata"]["wikidata"] = username

    for (statement, reason) in csv.reader(csv_file):
        process_statement(statement, reason)


def process_statement(statement: str, reason: str):
    (item, claim) = find_claim_by_guid(statement)
    assert item and claim

    reason_item: ItemPage = pywikibot.ItemPage(SITE, reason)
    assert reason_item

    claim.setRank("deprecated")

    if not claim.qualifiers.get(REASON_FOR_DEPRECATED_RANK_PID):
        qualifier = REASON_FOR_DEPRECATED_RANK_PROPERTY.newClaim()
        qualifier.isQualifier = True
        qualifier.setTarget(reason_item)
        claim.qualifiers[REASON_FOR_DEPRECATED_RANK_PID] = [qualifier]

    item.editEntity({"claims": [claim.toJSON()]})


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
