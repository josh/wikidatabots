# pyright: reportGeneralTypeIssues=false

import logging
import os
from collections import OrderedDict
from datetime import date
from typing import TypeVar

import polars as pl
import pywikibot
import pywikibot.config
from pywikibot import Claim, ItemPage, PropertyPage, WbQuantity, WbTime

from opencritic_etl import fetch_opencritic_game, opencritic_ratelimits
from polars_utils import sample
from pwb import login
from sparql import sparql
from wikidata import page_qids

SITE = pywikibot.Site("wikidata", "wikidata")

# Wikidata Property IDs
DETERMINATION_METHOD_PID = "P459"
NUMBER_OF_REVIEWS_RATINGS_PID = "P7887"
OPENCRITIC_ID_PID = "P2864"
POINT_IN_TIME_PID = "P585"
RETRIEVED_PID = "P813"
REVIEW_SCORE_BY_PID = "P447"
REVIEW_SCORE_PID = "P444"
STATED_IN_PID = "P248"

# Wikidata Item IDs
CRITIC_REVIEW_QID = "Q80698083"
OPENCRITIC_QID = "Q21039459"
OPENCRITIC_TOP_CRITIC_AVERAGE_QID = "Q114712322"

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

_QUERY = """
SELECT ?item ?opencritic_id ?statement ?reference
      ?review_score ?point_in_time ?number_of_reviews WHERE {
  ?item wdt:P2864 ?opencritic_id.
  FILTER(xsd:integer(?opencritic_id))

  OPTIONAL {
    ?item p:P444 ?statement.

    ?statement wikibase:rank ?rank.
    FILTER(?rank != wikibase:DeprecatedRank)

    ?statement ps:P444 ?review_score.
    ?statement pq:P447 wd:Q21039459.

    OPTIONAL { ?statement pq:P459 wd:Q114712322. }
    OPTIONAL { ?statement pq:P585 ?point_in_time. }
    OPTIONAL { ?statement pq:P7887 ?number_of_reviews. }

    OPTIONAL {
      ?statement prov:wasDerivedFrom ?reference.
      ?reference pr:P2864 ?opencritic_id.
      OPTIONAL { ?reference pr:P248 wd:Q21039459. }
      OPTIONAL { ?reference pr:P813 ?retrieved. }
    }
  }
}
"""

_QUERY_COLUMNS = [
    "item",
    "opencritic_id",
    "statement",
    "reference",
    "review_score",
    "point_in_time",
    "number_of_reviews",
]


def main() -> None:
    login(os.environ["WIKIDATA_USERNAME"], os.environ["WIKIDATA_PASSWORD"])

    ratelimits_df = opencritic_ratelimits().collect()
    requests_remaining = ratelimits_df["requests_remaining"].item()
    logging.info(f"OpenCritic API requests remaining: {requests_remaining}")

    if requests_remaining < 1:
        logging.warning("No available API requests for the day")
        return

    blocked_qids: set[str] = set(page_qids("User:Josh404Bot/Blocklist"))

    df = (
        sparql(_QUERY, columns=_QUERY_COLUMNS)
        .with_columns(
            pl.col("item")
            .str.replace("http://www.wikidata.org/entity/", "")
            .alias("qid")
        )
        .with_columns(
            pl.col("opencritic_id").cast(pl.UInt32).alias("opencritic_id"),
            pl.col("qid").is_in(blocked_qids).alias("blocked"),
        )
        .unique("item", keep="none")
        .filter(
            pl.col("opencritic_id").is_not_null() & pl.col("blocked").is_not(),
        )
        .pipe(sample, n=(requests_remaining / 2))
        .with_columns(
            pl.col("opencritic_id").pipe(fetch_opencritic_game).alias("api_data"),
        )
        .with_columns(
            (
                pl.col("api_data")
                .struct.field("top_critic_score")
                .round(0)
                .cast(pl.UInt8)
                .alias("api_top_critic_score")
            ),
            (
                pl.col("api_data")
                .struct.field("latest_review_date")
                .alias("api_latest_review_date")
            ),
            pl.col("api_data").struct.field("num_reviews").alias("api_num_reviews"),
        )
        .drop("api_data")
        .filter(
            pl.col("api_top_critic_score").is_not_null()
            & pl.col("api_latest_review_date").is_not_null()
            & (pl.col("api_num_reviews") > 0)
        )
        .select(
            "qid",
            "opencritic_id",
            "api_num_reviews",
            "api_top_critic_score",
            "api_latest_review_date",
        )
        .collect()
    )

    for row in df.iter_rows(named=True):
        _update_review_score_claim(
            item=ItemPage(SITE, row["qid"]),
            opencritic_id=row["opencritic_id"],
            number_of_reviews=row["api_num_reviews"],
            top_critic_score=row["api_top_critic_score"],
            latest_review_date=row["api_latest_review_date"],
        )


def _update_review_score_claim(
    item: ItemPage,
    opencritic_id: int,
    top_critic_score: float,
    number_of_reviews: int,
    latest_review_date: date,
) -> None:
    claim: Claim = REVIEW_SCORE_CLAIM.copy()
    orig_claim: Claim | None = None

    # Find existing review score claim, if one exists
    for c in item.claims.get(REVIEW_SCORE_PID, []):
        if c.has_qualifier(REVIEW_SCORE_BY_PID, OPENCRITIC_QID):
            claim = c
            orig_claim = c.copy()

    # Update review score value top OpenCritic top-critic score
    claim.setTarget("{}/100".format(top_critic_score))

    # Set determination method to "top critic average"
    claim.qualifiers[DETERMINATION_METHOD_PID] = [DETERMINATION_METHOD_CLAIM.copy()]

    # Update point in time qualifier
    point_in_time_qualifier = find_or_initialize_qualifier(
        claim, POINT_IN_TIME_PROPERTY
    )
    point_in_time_wbtime = WbTime.fromTimestr(
        f"{latest_review_date}T00:00:00Z", precision=11
    )
    point_in_time_qualifier.setTarget(point_in_time_wbtime)

    # Update number of critic reviewers qualifier
    number_of_reviews_qualifier = find_or_initialize_qualifier(
        claim, NUMBER_OF_REVIEWS_RATINGS_PROPERTY
    )
    number_of_reviews_quantity = WbQuantity(
        amount=number_of_reviews, unit=CRITIC_REVIEW_ITEM, site=item.repo
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
