import logging

from tqdm import tqdm

import appletv
from page import blocked_qids, page_qids
from sparql import fetch_statements, sample_items, type_constraints


def main():
    """
    Find Wikidata items that are missing a iTunes movie ID (P6398) but have a
    Apple TV movie ID (P9586).

    Outputs QuickStatements CSV commands.
    """

    qids = page_qids("User:Josh404Bot/Preliminarily matched/P6398")
    qids |= sample_items("P9586", limit=1000)

    allowed_classes = type_constraints("P6398")
    results = fetch_statements(qids, ["P31", "P6398", "P9586"])

    print("qid,P6398")
    for qid in tqdm(results):
        item = results[qid]

        if qid in blocked_qids():
            logging.debug("{} is blocked".format(qid))
            continue

        if not item.get("P31") or item.get("P6398"):
            continue

        instance_of = set([v for (_, v) in item["P31"]])
        if instance_of.isdisjoint(allowed_classes):
            continue

        for (statement, value) in item.get("P9586", []):
            movie = appletv.movie(value)
            if movie and movie["itunes_id"]:
                print('{},"""{}"""'.format(qid, movie["itunes_id"]))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
