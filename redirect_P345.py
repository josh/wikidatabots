import logging

import pywikibot  # type: ignore
import pywikibot.config  # type: ignore
from tqdm import tqdm

import imdb
from sparql import sample_items

REASON_FOR_DEPRECATION = "P2241"
REDIRECT = "Q45403344"


def main():
    pywikibot.config.usernames["wikidata"]["wikidata"] = "Josh404"
    site = pywikibot.Site("wikidata", "wikidata")
    repo = site.data_repository()

    qids = sample_items("P345", limit=10)

    redirect_page = pywikibot.ItemPage(repo, REDIRECT)

    for qid in tqdm(qids):
        item = pywikibot.ItemPage(repo, qid)

        if item.isRedirectPage():
            logging.debug(f"{item} is a redirect")
            continue

        for claim in item.claims.get("P345", []):  # type: ignore
            id = claim.target  # type: ignore
            assert type(id) is str  # type: ignore

            if claim.rank == "deprecated":  # type: ignore
                continue

            if not imdb.is_valid_id(id):
                logging.debug(f"{id} is invalid format")
                continue

            new_id = imdb.canonical_id(id)
            if not new_id:
                logging.debug(f"{id} not found")
                continue

            if id is not new_id:
                claim.setRank("deprecated")  # type: ignore
                qualifier = pywikibot.Claim(repo, REASON_FOR_DEPRECATION)
                qualifier.isQualifier = True
                qualifier.setTarget(redirect_page)  # type: ignore
                claim.qualifiers[REASON_FOR_DEPRECATION] = [qualifier]  # type: ignore

                if claim_exists(item, "P345", new_id):
                    item.editEntity({"claims": [claim.toJSON()]})  # type: ignore
                else:
                    new_claim = pywikibot.Claim(repo, "P345")
                    new_claim.setTarget(new_id)  # type: ignore
                    item.editEntity(  # type: ignore
                        {
                            "claims": [
                                new_claim.toJSON(),  # type: ignore
                                claim.toJSON(),  # type: ignore
                            ],
                        }
                    )


def claim_exists(page: pywikibot.ItemPage, property: str, value: str) -> bool:
    for claim in page.claims[property]:  # type: ignore
        if claim.target == value:  # type: ignore
            return True
    return False


if __name__ == "__main__":
    main()
