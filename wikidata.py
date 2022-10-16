import os

import pywikibot.config
from pywikibot import Claim, ItemPage, PropertyPage, Site

SITE = Site("wikidata", "wikidata")

if "WIKIDATA_USERNAME" in os.environ:
    pywikibot.config.usernames["wikidata"]["wikidata"] = os.environ["WIKIDATA_USERNAME"]

pywikibot.config.password_file = "user-password.py"


def find_or_initialize_qualifier(claim: Claim, property: PropertyPage) -> Claim:
    for qualifier in claim.qualifiers.get(property.id, []):
        return qualifier
    qualifier = property.newClaim(is_qualifier=True)
    claim.qualifiers[property.id] = [qualifier]
    return qualifier


def find_claim_by_guid(guid: str) -> tuple[ItemPage, Claim] | tuple[None, None]:
    assert "$" in guid
    qid = guid.split("$", 1)[0]
    assert qid.startswith("Q")
    item = ItemPage(SITE, qid)

    for property in item.claims:
        for claim in item.claims[property]:
            if guid == claim.snak:
                return (item, claim)

    return (None, None)
