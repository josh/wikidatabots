"""
Batch Wikidata statements uploader. Similar to QuickStatements.

MUST be logged in first. See pwb.py
"""

import csv
import io

import pywikibot


def process_batch(username, data):
    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    for (entity, property, value) in data_reader(data):
        if "$" in entity:
            process_statement(repo, entity, property, value)


def data_reader(data):
    f = io.StringIO()
    if type(data) is str:
        f.write(data)
    else:
        for line in data:
            f.write("{}\n".format(line))
    f.seek(0)
    return csv.reader(f)


def process_statement(repo, guid, pq, value):
    (item, claim) = find_claim(repo, guid)

    if property == "rank":
        if claim.rank != value:
            claim.setRank(value)
            item.editEntity({"claims": [claim.toJSON()]})
        return

    target = parse_value(repo, value)

    qualifiers = claim.qualifiers[pq]
    if qualifiers:
        if target != qualifiers[0].target:
            qualifiers[0].setTarget(target)
            item.editEntity({"claims": [claim.toJSON()]})
    else:
        qualifier = pywikibot.Claim(repo, pq)
        qualifier.setTarget(target)
        claim.addQualifier(qualifier)


def find_claim(repo, guid):
    qid = guid.split("$", 1)[0]
    item = pywikibot.ItemPage(repo, qid)
    properties = item.get()["claims"]

    for property in properties:
        for claim in properties[property]:
            if guid == claim.snak:
                return (item, claim)

    return (None, None)


def parse_value(repo, value):
    if value.startswith("Q"):
        item = pywikibot.ItemPage(repo, value)
        assert item
        return item
    else:
        return value


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
        data=sys.stdin.read(),
    )
