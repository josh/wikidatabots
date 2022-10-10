import logging

import pywikibot
import pywikibot.config
from tqdm import tqdm

import imdb
from items import REDIRECT_QID
from properties import REASON_FOR_DEPRECATED_RANK_PID
from sparql import sample_items


def main():
    pywikibot.config.usernames["wikidata"]["wikidata"] = "Josh404"
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    qids = sample_items("P345", limit=10)

    redirect_page = pywikibot.ItemPage(repo, REDIRECT_QID)

    for qid in tqdm(qids):
        item = pywikibot.ItemPage(repo, qid)

        if item.isRedirectPage():
            logging.debug(f"{item} is a redirect")
            continue

        for claim in item.claims.get("P345", []):
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
                qualifier.setTarget(redirect_page)
                claim.qualifiers[REASON_FOR_DEPRECATED_RANK_PID] = [qualifier]

                if claim_exists(item, "P345", new_id):
                    item.editEntity({"claims": [claim.toJSON()]})
                else:
                    new_claim = pywikibot.Claim(repo, "P345")
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
