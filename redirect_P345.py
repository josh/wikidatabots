import logging

import pywikibot
import pywikibot.config
from tqdm import tqdm

import imdb
from items import REDIRECT_ITEM
from properties import IMDB_ID_PID, REASON_FOR_DEPRECATED_RANK_PID
from sparql import sample_items
from wikidata import SITE


def main():
    pywikibot.config.usernames["wikidata"]["wikidata"] = "Josh404"
    repo = SITE.data_repository()

    qids = sample_items(IMDB_ID_PID, limit=10)

    for qid in tqdm(qids):
        item = pywikibot.ItemPage(repo, qid)

        if item.isRedirectPage():
            logging.debug(f"{item} is a redirect")
            continue

        for claim in item.claims.get(IMDB_ID_PID, []):
            id = claim.target
            assert type(id) is str

            if claim.rank == "deprecated":
                continue

            if not imdb.is_valid_id(id):
                logging.debug(f"{id} is invalid format")
                continue

            new_id = imdb.canonical_id(id)
            if not new_id:
                logging.debug(f"{id} not found")
                continue

            if id is not new_id:
                claim.setRank("deprecated")
                qualifier = pywikibot.Claim(repo, REASON_FOR_DEPRECATED_RANK_PID)
                qualifier.isQualifier = True
                qualifier.setTarget(REDIRECT_ITEM)
                claim.qualifiers[REASON_FOR_DEPRECATED_RANK_PID] = [qualifier]

                if claim_exists(item, IMDB_ID_PID, new_id):
                    item.editEntity({"claims": [claim.toJSON()]})
                else:
                    new_claim = pywikibot.Claim(repo, IMDB_ID_PID)
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
