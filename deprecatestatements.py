"""
Batch Wikidata statements deprecator.

MUST be logged in first. See pwb.py
"""

import csv
from typing import TextIO

import pywikibot
import pywikibot.config

from constants import NORMAL_RANK_QID, REASON_FOR_DEPRECATED_RANK_PID

SITE = pywikibot.Site("wikidata", "wikidata")

REASON_FOR_DEPRECATED_RANK_PROPERTY = pywikibot.PropertyPage(
    SITE, REASON_FOR_DEPRECATED_RANK_PID
)


def process_batch(username: str, csv_file: TextIO):
    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    pywikibot.config.password_file = "user-password.py"

    for (statement, reason) in csv.reader(csv_file):
        process_statement(statement, reason)


def process_statement(statement: str, reason: str):
    (item, claim) = find_claim_by_guid(statement)
    assert item and claim

    if reason == NORMAL_RANK_QID:
        claim.setRank("normal")
        claim.qualifiers[REASON_FOR_DEPRECATED_RANK_PID] = []
    else:
        claim.setRank("deprecated")

        if not claim.qualifiers.get(REASON_FOR_DEPRECATED_RANK_PID):
            qualifier = REASON_FOR_DEPRECATED_RANK_PROPERTY.newClaim()
            qualifier.isQualifier = True
            qualifier.setTarget(pywikibot.ItemPage(SITE, reason))
            claim.qualifiers[REASON_FOR_DEPRECATED_RANK_PID] = [qualifier]

    item.editEntity({"claims": [claim.toJSON()]})


def find_claim_by_guid(
    guid: str,
) -> tuple[pywikibot.ItemPage, pywikibot.Claim] | tuple[None, None]:
    assert "$" in guid
    qid = guid.split("$", 1)[0]
    assert qid.startswith("Q")
    item = pywikibot.ItemPage(SITE, qid)

    for property in item.claims:
        for claim in item.claims[property]:
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
