# pyright: reportGeneralTypeIssues=false

import logging
import os
from collections import OrderedDict
from datetime import date, datetime
from typing import TypeVar

import polars as pl
import pywikibot
import pywikibot.config
from pywikibot import Claim, ItemPage, PropertyPage, WbQuantity, WbTime
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from sparql import sparql
from wikidata import is_blocked_item

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

_QUERY_SCHEMA = {
    "item": pl.Utf8,
    "opencritic_id": pl.UInt32,
    "statement": pl.Utf8,
    "reference": pl.Utf8,
    "review_score": pl.Utf8,
    "point_in_time": pl.Utf8,
    "number_of_reviews": pl.Float64,
}


def main() -> None:
    wd_df = (
        sparql(_QUERY, schema=_QUERY_SCHEMA)
        .unique("item", keep="none")
        .with_columns(
            pl.col("item")
            .str.replace("http://www.wikidata.org/entity/", "")
            .alias("qid")
        )
        .with_columns(
            pl.col("number_of_reviews").cast(pl.UInt16),
            pl.col("point_in_time").str.strptime(pl.Date, "%+"),
        )
        .select(pl.all().prefix("wd_"))
    )

    api_df = pl.scan_parquet(
        "s3://wikidatabots/opencritic.parquet",
        storage_options={"anon": True},
    ).select(pl.all().prefix("api_"))

    df = (
        wd_df.join(api_df, left_on="wd_opencritic_id", right_on="api_id", how="left")
        .filter(
            pl.col("wd_qid").pipe(is_blocked_item).is_not()
            & pl.col("api_top_critic_score").is_not_null()
            & pl.col("api_latest_review_date").is_not_null()
            & pl.col("api_retrieved_at").is_not_null()
            & (pl.col("api_num_reviews") > 0)
        )
        .with_columns(
            pl.format(
                "{}/100", pl.col("api_top_critic_score").round(0).cast(pl.UInt8)
            ).alias("api_review_score"),
        )
        .filter(
            pl.col("wd_review_score").is_null()
            | pl.col("wd_point_in_time").is_null()
            | pl.col("wd_number_of_reviews").is_null()
            | (pl.col("wd_review_score") != pl.col("api_review_score"))
            | ((pl.col("wd_number_of_reviews") + 10) < pl.col("api_num_reviews"))
        )
        .select(
            "wd_qid",
            "wd_opencritic_id",
            "api_review_score",
            "api_num_reviews",
            "api_latest_review_date",
            "api_retrieved_at",
        )
        # MARK: pl.LazyFrame.collect
        .collect()
    )

    rows = list(df.iter_rows(named=True))
    with logging_redirect_tqdm():
        for row in tqdm(rows, unit="row"):
            _update_review_score_claim(
                item=ItemPage(SITE, row["wd_qid"]),
                opencritic_id=row["wd_opencritic_id"],
                review_score=row["api_review_score"],
                number_of_reviews=row["api_num_reviews"],
                latest_review_date=row["api_latest_review_date"],
                retrieved_at=row["api_retrieved_at"],
            )


def _update_review_score_claim(
    item: ItemPage,
    opencritic_id: int,
    review_score: str,
    number_of_reviews: int,
    latest_review_date: date,
    retrieved_at: datetime,
) -> None:
    claim: Claim = REVIEW_SCORE_CLAIM.copy()

    # Find existing review score claim, if one exists
    for c in item.claims.get(REVIEW_SCORE_PID, []):
        if c.has_qualifier(REVIEW_SCORE_BY_PID, OPENCRITIC_QID):
            claim = c

    # Update review score value top OpenCritic top-critic score
    claim.setTarget(review_score)

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

    opencritic_id_reference = OPENCRITIC_ID_PROPERTY.newClaim(is_reference=True)
    opencritic_id_reference.setTarget(f"{opencritic_id}")

    retrieved_reference = None
    for source in claim.getSources():
        if has_claim(opencritic_id_reference, source):
            retrieved_reference = get_dict_value(source, RETRIEVED_PID)

    retrieved_at_wbtime = WbTime(
        year=retrieved_at.year,
        month=retrieved_at.month,
        day=retrieved_at.day,
        precision=11,
    )

    # Update existing retrieved reference, or create a new one
    if retrieved_reference:
        retrieved_reference.setTarget(retrieved_at_wbtime)
    else:
        retrieved_reference = RETRIEVED_PROPERTY.newClaim(is_reference=True)
        retrieved_reference.setTarget(retrieved_at_wbtime)

        references = [
            (STATED_IN_PID, [STATED_IN_REFERENCE.copy()]),
            (OPENCRITIC_ID_PID, [opencritic_id_reference]),
            (RETRIEVED_PID, [retrieved_reference]),
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


T = TypeVar("T")


def get_dict_value(dict: OrderedDict[str, list[T]], key: str) -> T | None:
    for value in dict.get(key, []):
        return value
    return None


def login() -> None:
    username = os.environ["WIKIDATA_USERNAME"]
    password = os.environ["WIKIDATA_PASSWORD"]

    with open(pywikibot.config.password_file, "w") as file:
        file.write(f'("{username}", "{password}")')
    os.chmod(pywikibot.config.password_file, 0o600)

    SITE.login()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    login()
    main()
