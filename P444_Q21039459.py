import logging
import os
from collections import OrderedDict
from datetime import date
from operator import is_
from typing import Optional, TypeVar

import pywikibot
import pywikibot.config
from pywikibot import Claim, ItemPage, WbQuantity, WbTime
from tqdm import tqdm

from items import (
    CRITIC_REVIEW_ITEM,
    OPENCRITIC_ITEM,
    OPENCRITIC_QID,
    OPENCRITIC_TOP_CRITIC_AVERAGE_ITEM,
)
from opencritic import fetch_game
from page import blocked_qids
from properties import (
    DETERMINATION_METHOD_PID,
    DETERMINATION_METHOD_PROPERTY,
    NUMBER_OF_REVIEWS_RATINGS_PROPERTY,
    OPENCRITIC_ID_PID,
    OPENCRITIC_ID_PROPERTY,
    POINT_IN_TIME_PROPERTY,
    RETRIEVED_PID,
    RETRIEVED_PROPERTY,
    REVIEW_SCORE_BY_PID,
    REVIEW_SCORE_BY_PROPERTY,
    REVIEW_SCORE_PID,
    REVIEW_SCORE_PROPERTY,
    STATED_IN_PID,
)
from sparql import sparql
from utils import tryint
from wikidata import SITE, find_or_initialize_qualifier

REVIEW_SCORE_CLAIM = REVIEW_SCORE_PROPERTY.newClaim()

REVIEW_SCORE_BY_CLAIM = REVIEW_SCORE_BY_PROPERTY.newClaim(is_qualifier=True)
REVIEW_SCORE_BY_CLAIM.setTarget(OPENCRITIC_ITEM)
DETERMINATION_METHOD_CLAIM = DETERMINATION_METHOD_PROPERTY.newClaim(is_qualifier=True)
REVIEW_SCORE_BY_CLAIM.setTarget(OPENCRITIC_TOP_CRITIC_AVERAGE_ITEM)
REVIEW_SCORE_CLAIM.qualifiers[REVIEW_SCORE_BY_PID] = [REVIEW_SCORE_BY_CLAIM]
REVIEW_SCORE_CLAIM.qualifiers[DETERMINATION_METHOD_PID] = [DETERMINATION_METHOD_CLAIM]

TODAY_DATE = date.today()
TODAY_WBTIME = WbTime(
    year=TODAY_DATE.year,
    month=TODAY_DATE.month,
    day=TODAY_DATE.day,
    precision=11,
)
RETRIEVED_TODAY_REFERENCE = RETRIEVED_PROPERTY.newClaim(is_reference=True)
RETRIEVED_TODAY_REFERENCE.setTarget(TODAY_WBTIME)

pywikibot.config.usernames["wikidata"]["wikidata"] = os.environ["WIKIDATA_USERNAME"]


def main():
    query = """
    SELECT DISTINCT ?item ?opencritic ?random WHERE {
      ?item wdt:P2864 ?opencritic.

      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY (?random)
    LIMIT 10
    """
    results = sparql(query)

    for result in tqdm(results):
        qid: str = result["item"]

        if qid in blocked_qids():
            logging.warn(f"{qid} is blocked")
            continue

        item = ItemPage(SITE, qid)
        update_review_score_claim(item)


def update_review_score_claim(item: ItemPage):
    opencritic_id = find_opencritic_id(item)
    if not opencritic_id:
        logging.info(f"Skipping {item.id}, has no OpenCritic ID")
        return

    # Fetch latest data from OpenCritic API
    data = fetch_game(opencritic_id)

    claim: Claim = REVIEW_SCORE_CLAIM.copy()
    orig_claim: Optional[Claim] = None

    # Find existing review score claim, if one exists
    for c in item.claims.get(REVIEW_SCORE_PID, []):
        if c.has_qualifier(REVIEW_SCORE_BY_PID, OPENCRITIC_QID):
            claim = c.copy()
            orig_claim = c

    # Update review score value top OpenCritic top-critic score
    claim.setTarget("{}/100".format(round(data["topCriticScore"])))

    # Set determination method to "top critic average"
    claim.qualifiers[DETERMINATION_METHOD_PID] = [DETERMINATION_METHOD_CLAIM.copy()]

    # Update point in time qualifier
    point_in_time_qualifier = find_or_initialize_qualifier(
        claim, POINT_IN_TIME_PROPERTY
    )
    point_in_time_wbtime = WbTime.fromTimestr(
        data["latestReviewDate"].replace(".000Z", "Z"), precision=11
    )
    point_in_time_qualifier.setTarget(point_in_time_wbtime)

    # Update number of critic reviewers qualifier
    number_of_reviews_qualifier = find_or_initialize_qualifier(
        claim, NUMBER_OF_REVIEWS_RATINGS_PROPERTY
    )
    number_of_reviews_quantity = WbQuantity(
        amount=data["numReviews"], unit=CRITIC_REVIEW_ITEM, site=item.repo
    )
    number_of_reviews_qualifier.setTarget(number_of_reviews_quantity)

    # Skip editting if claims are the same
    if compare_claims(claim, orig_claim):
        logging.info(f"Skipping {item.id}, review score is up to date")
        return

    opencritic_id_reference = OPENCRITIC_ID_PROPERTY.newClaim(is_reference=True)
    opencritic_id_reference.setTarget(f"{opencritic_id}")

    retrieved_reference = None
    for source in claim.getSources():
        if has_claim(opencritic_id_reference, source):
            retrieved_reference = get_dict_value(source, RETRIEVED_PID)

    # Update existing retrieved reference, or create a new one
    if retrieved_reference:
        retrieved_reference.setTarget(TODAY_WBTIME)
    else:
        references = [
            (STATED_IN_PID, [OPENCRITIC_ITEM]),
            (OPENCRITIC_ID_PID, [opencritic_id_reference]),
            (RETRIEVED_PID, [RETRIEVED_TODAY_REFERENCE.copy()]),
        ]
        claim.sources.append(OrderedDict(references))

    assert claim.toJSON()
    logging.info(f"Editting {item.id}")
    item.editEntity({"claims": [claim.toJSON()]})


def find_opencritic_id(item: ItemPage) -> Optional[int]:
    claim = get_dict_value(item.claims, OPENCRITIC_ID_PID)
    if not claim:
        return None
    return tryint(claim.target)


def has_claim(claim: Claim, claims: OrderedDict[str, list[Claim]]) -> bool:
    return any(c for c in claims[claim.id] if c.same_as(claim))


def compare_claims(a: Claim, b: Claim | None) -> bool:
    if not b:
        return False
    return a.same_as(b, ignore_rank=True, ignore_quals=False, ignore_refs=True)


T = TypeVar("T")


def get_dict_value(dict: OrderedDict[str, list[T]], key: str) -> Optional[T]:
    for value in dict.get(key, []):
        return value
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
