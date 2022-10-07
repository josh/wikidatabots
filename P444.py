import logging
import os
from collections import OrderedDict
from datetime import date
from typing import Any, Optional, TypedDict, TypeVar

import pywikibot
import pywikibot.config
import requests
from pywikibot import Claim, ItemPage, PropertyPage, Site, WbQuantity, WbTime
from tqdm import tqdm

from page import blocked_qids
from sparql import sparql

SITE = Site("wikidata", "wikidata")
REVIEW_SCORE_PID = "P444"
REVIEW_SCORE_PROPERTY = PropertyPage(SITE, "P444")
REVIEW_SCORE_BY_PID = "P447"
REVIEW_SCORE_BY_PROPERTY = PropertyPage(SITE, "P447")
RETRIEVED_PID = "P813"
RETRIEVED_PROPERTY = PropertyPage(SITE, "P813")
OPENCRITIC_ID_PID = "P2864"
OPENCRITIC_ID_PROPERTY = PropertyPage(SITE, "P2864")
NUMBER_OF_REVIEWS_RATINGS_PID = "P7887"
NUMBER_OF_REVIEWS_RATINGS_PROPERTY = PropertyPage(SITE, "P7887")
OPENCRITIC_QID = "Q21039459"
OPENCRITIC_ITEM = ItemPage(SITE, "Q21039459")
CRITIC_REVIEW_ITEM = ItemPage(SITE, "Q80698083")

RAPIDAPI_HEADERS = {
    "X-RapidAPI-Key": os.environ["RAPIDAPI_KEY"],
    "X-RapidAPI-Host": "opencritic-api.p.rapidapi.com",
}

REVIEW_SCORE_CLAIM = REVIEW_SCORE_PROPERTY.newClaim()
REVIEW_SCORE_BY_CLAIM = REVIEW_SCORE_BY_PROPERTY.newClaim(is_qualifier=True)
REVIEW_SCORE_BY_CLAIM.setTarget(OPENCRITIC_ITEM)
REVIEW_SCORE_CLAIM.qualifiers[REVIEW_SCORE_BY_PID] = [REVIEW_SCORE_BY_CLAIM]

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
    LIMIT 5
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

    # Find or initialize number of reviews/ratings qualifier
    number_of_reviews_qualifier = get_dict_value(
        claim.qualifiers, NUMBER_OF_REVIEWS_RATINGS_PID
    )
    if not number_of_reviews_qualifier:
        number_of_reviews_qualifier = NUMBER_OF_REVIEWS_RATINGS_PROPERTY.newClaim(
            is_qualifier=True
        )
        claim.qualifiers[NUMBER_OF_REVIEWS_RATINGS_PID] = [number_of_reviews_qualifier]

    # Update number of critic reviewers qualifier
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


class OpenCriticGame(TypedDict):
    numReviews: int
    topCriticScore: float


def fetch_game(game_id: int) -> OpenCriticGame:
    url = f"https://opencritic-api.p.rapidapi.com/game/{game_id}"
    response = requests.get(url, headers=RAPIDAPI_HEADERS, timeout=5)
    return response.json()


T = TypeVar("T")


def get_dict_value(dict: OrderedDict[str, list[T]], key: str) -> Optional[T]:
    for value in dict.get(key, []):
        return value
    return None


def tryint(value: Any) -> Optional[int]:
    try:
        return int(value)
    except ValueError:
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
