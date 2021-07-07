import logging

import pywikibot
from tqdm import tqdm

import imdb
from page import page_qids
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
            logging.debug("{} is a redirect".format(item))
            continue

        for claim in item.claims.get("P345", []):
            id = claim.target

            if claim.rank == "deprecated":
                continue

            if not imdb.is_valid_id(id):
                logging.debug("{} is invalid format".format(id))
                continue

            new_id = imdb.canonical_id(id)
            if not new_id:
                logging.debug("{} not found".format(id))
                continue

            if id is not new_id:
                claim.setRank("deprecated")
                qualifier = pywikibot.Claim(repo, REASON_FOR_DEPRECATION)
                qualifier.isQualifier = True
                qualifier.setTarget(redirect_page)
                claim.qualifiers[REASON_FOR_DEPRECATION] = [qualifier]

                if claim_exists(item, "P345", new_id):
                    item.editEntity({"claims": [claim.toJSON()]})
                else:
                    new_claim = pywikibot.Claim(repo, "P345")
                    new_claim.setTarget(new_id)
                    item.editEntity({"claims": [new_claim.toJSON(), claim.toJSON()]})


def claim_exists(page, property, value):
    for claim in page.claims[property]:
        if claim.target == value:
            return True
    return False


if __name__ == "__main__":
    main()
