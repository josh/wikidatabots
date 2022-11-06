import logging

import pywikibot
import pywikibot.config
from tqdm import tqdm

import imdb
from constants import IMDB_ID_PID, REASON_FOR_DEPRECATED_RANK_PID, REDIRECT_QID
from sparql import sample_items

SITE = pywikibot.Site("wikidata", "wikidata")
REASON_FOR_DEPRECATED_RANK_PROPERTY = pywikibot.PropertyPage(
    SITE, REASON_FOR_DEPRECATED_RANK_PID
)
REDIRECT_ITEM = pywikibot.ItemPage(SITE, REDIRECT_QID)
IMDB_ID_PROPERTY = pywikibot.PropertyPage(SITE, IMDB_ID_PID)


def main():
    pywikibot.config.usernames["wikidata"]["wikidata"] = "Josh404"

    qids = sample_items(IMDB_ID_PID, limit=10)

    for qid in tqdm(qids):
        item = pywikibot.ItemPage(SITE, qid)

        if item.isRedirectPage():
            logging.debug(f"{item} is a redirect")
            continue

        for claim in item.claims.get(IMDB_ID_PID, []):
            id = claim.target
            assert type(id) is str

            if claim.rank == "deprecated":
                continue

            id = imdb.tryid(id)
            if not id:
                logging.debug(f"{id} is invalid format")
                continue

            new_id = imdb.canonical_id(id)
            if not new_id:
                logging.debug(f"{id} not found")
                continue

            if id is not new_id:
                claim.setRank("deprecated")
                qualifier = REASON_FOR_DEPRECATED_RANK_PROPERTY.newClaim()
                qualifier.isQualifier = True
                qualifier.setTarget(REDIRECT_ITEM)
                claim.qualifiers[REASON_FOR_DEPRECATED_RANK_PID] = [qualifier]

                if claim_exists(item, IMDB_ID_PID, new_id):
                    item.editEntity({"claims": [claim.toJSON()]})
                else:
                    new_claim = IMDB_ID_PROPERTY.newClaim()
                    new_claim.setTarget(new_id)
                    item.editEntity(
                        {
                            "claims": [
                                new_claim.toJSON(),
                                claim.toJSON(),
                            ],
                        }
                    )


def claim_exists(page: pywikibot.ItemPage, property: str, value: str) -> bool:
    for claim in page.claims[property]:
        if claim.target == value:
            return True
    return False


if __name__ == "__main__":
    main()
