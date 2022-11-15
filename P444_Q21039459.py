# pyright: basic

import itertools
import logging
import os
from collections import OrderedDict
from datetime import date
from typing import TypedDict, TypeVar

import pywikibot
import pywikibot.config
from pywikibot import Claim, ItemPage, PropertyPage, WbQuantity, WbTime

import opencritic
import wikidata
from constants import (
    CRITIC_REVIEW_QID,
    DETERMINATION_METHOD_PID,
    NUMBER_OF_REVIEWS_RATINGS_PID,
    OPENCRITIC_ID_PID,
    OPENCRITIC_QID,
    OPENCRITIC_TOP_CRITIC_AVERAGE_QID,
    POINT_IN_TIME_PID,
    RETRIEVED_PID,
    REVIEW_SCORE_BY_PID,
    REVIEW_SCORE_PID,
    STATED_IN_PID,
)
from opencritic import fetch_game
from page import filter_blocked_qids
from sparql import sparql
from timeout import iter_until_deadline
from utils import position_weighted_shuffled, tryint

SITE = pywikibot.Site("wikidata", "wikidata")

CRITIC_REVIEW_ITEM = ItemPage(SITE, CRITIC_REVIEW_QID)
OPENCRITIC_ITEM = ItemPage(SITE, OPENCRITIC_QID)
OPENCRITIC_TOP_CRITIC_AVERAGE_ITEM = ItemPage(SITE, OPENCRITIC_TOP_CRITIC_AVERAGE_QID)

DETERMINATION_METHOD_PROPERTY = PropertyPage(SITE, DETERMINATION_METHOD_PID)
NUMBER_OF_REVIEWS_RATINGS_PROPERTY = PropertyPage(SITE, NUMBER_OF_REVIEWS_RATINGS_PID)
OPENCRITIC_ID_PROPERTY = PropertyPage(SITE, OPENCRITIC_ID_PID)
POINT_IN_TIME_PROPERTY = PropertyPage(SITE, POINT_IN_TIME_PID)
RETRIEVED_PROPERTY = PropertyPage(SITE, RETRIEVED_PID)
REVIEW_SCORE_BY_PROPERTY = PropertyPage(SITE, REVIEW_SCORE_BY_PID)
REVIEW_SCORE_PROPERTY = PropertyPage(SITE, REVIEW_SCORE_PID)
STATED_IN_PROPERTY = PropertyPage(SITE, STATED_IN_PID)

REVIEW_SCORE_CLAIM = REVIEW_SCORE_PROPERTY.newClaim()

REVIEW_SCORE_BY_CLAIM = REVIEW_SCORE_BY_PROPERTY.newClaim(is_qualifier=True)
REVIEW_SCORE_BY_CLAIM.setTarget(OPENCRITIC_ITEM)
DETERMINATION_METHOD_CLAIM = DETERMINATION_METHOD_PROPERTY.newClaim(is_qualifier=True)
DETERMINATION_METHOD_CLAIM.setTarget(OPENCRITIC_TOP_CRITIC_AVERAGE_ITEM)
REVIEW_SCORE_CLAIM.qualifiers[REVIEW_SCORE_BY_PID] = [REVIEW_SCORE_BY_CLAIM]
REVIEW_SCORE_CLAIM.qualifiers[DETERMINATION_METHOD_PID] = [DETERMINATION_METHOD_CLAIM]

STATED_IN_REFERENCE = STATED_IN_PROPERTY.newClaim(is_reference=True)
STATED_IN_REFERENCE.setTarget(OPENCRITIC_ITEM)

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
pywikibot.config.password_file = "user-password.py"
pywikibot.config.put_throttle = 0


def main():
    qids = fetch_game_qids()
    qids = position_weighted_shuffled(qids)
    qids = itertools.islice(qids, 500)

    for qid in iter_until_deadline(qids):
        item = ItemPage(SITE, qid)
        try:
            update_review_score_claim(item)
        except opencritic.RatelimitException as e:
            logging.error(e)
            break


def fetch_game_qids() -> list[wikidata.QID]:
    query = """
    SELECT ?item WHERE {
      ?item wdt:P2864 ?opencritic.
      OPTIONAL {
        ?item p:P444 ?statement.
        ?statement pq:P447 wd:Q21039459.
        ?statement pq:P585 ?pointInTime.
      }
      FILTER((?pointInTime < NOW() - "P7D"^^xsd:duration) || !(BOUND(?pointInTime)))
      BIND(IF(BOUND(?pointInTime), ?pointInTime, NOW()) AS ?timestamp)
    }
    ORDER BY DESC (?timestamp)
    """

    class Result(TypedDict):
        item: wikidata.QID

    results: list[Result] = sparql(query)
    qids = [result["item"] for result in results]
    return list(filter_blocked_qids(qids))


def update_review_score_claim(item: ItemPage):
    opencritic_id = find_opencritic_id(item)
    if not opencritic_id:
        logging.warning(f"Skipping {item.id}, has no OpenCritic ID")
        return

    # Fetch latest data from OpenCritic API
    data = fetch_game(opencritic_id)

    claim: Claim = REVIEW_SCORE_CLAIM.copy()
    orig_claim: Claim | None = None

    # Find existing review score claim, if one exists
    for c in item.claims.get(REVIEW_SCORE_PID, []):
        if c.has_qualifier(REVIEW_SCORE_BY_PID, OPENCRITIC_QID):
            claim = c
            orig_claim = c.copy()

    if data["topCriticScore"] <= 0:
        logging.debug(f"Skipping {item.id}, has no score")
        return

    # Update review score value top OpenCritic top-critic score
    claim.setTarget("{}/100".format(round(data["topCriticScore"])))

    # Set determination method to "top critic average"
    claim.qualifiers[DETERMINATION_METHOD_PID] = [DETERMINATION_METHOD_CLAIM.copy()]

    # Update point in time qualifier
    point_in_time_qualifier = find_or_initialize_qualifier(
        claim, POINT_IN_TIME_PROPERTY
    )
    point_in_time_wbtime = WbTime.fromTimestr(
        data["latestReviewDate"][0:10] + "T00:00:00Z", precision=11
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
        logging.debug(f"Skipping {item.id}, review score is up to date")
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
            (STATED_IN_PID, [STATED_IN_REFERENCE.copy()]),
            (OPENCRITIC_ID_PID, [opencritic_id_reference]),
            (RETRIEVED_PID, [RETRIEVED_TODAY_REFERENCE.copy()]),
        ]
        claim.sources.append(OrderedDict(references))

    assert claim.toJSON()
    logging.info(f"Editting {item.id}")
    item.editEntity(
        {"claims": [claim.toJSON()]},
        summary="Update OpenCritic review score",
    )


def find_opencritic_id(item: ItemPage) -> int | None:
    claim = get_dict_value(item.claims, OPENCRITIC_ID_PID)
    if not claim:
        return None
    return tryint(claim.target)


def find_or_initialize_qualifier(claim: Claim, property: PropertyPage) -> Claim:
    for qualifier in claim.qualifiers.get(property.id, []):
        return qualifier
    qualifier = property.newClaim(is_qualifier=True)
    claim.qualifiers[property.id] = [qualifier]
    return qualifier


def has_claim(claim: Claim, claims: OrderedDict[str, list[Claim]]) -> bool:
    return any(c for c in claims.get(claim.id, []) if c.same_as(claim))


def compare_claims(a: Claim, b: Claim | None) -> bool:
    if not b:
        return False
    return a.same_as(b, ignore_rank=True, ignore_quals=False, ignore_refs=True)


T = TypeVar("T")


def get_dict_value(dict: OrderedDict[str, list[T]], key: str) -> T | None:
    for value in dict.get(key, []):
        return value
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
