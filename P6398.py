import logging

from tqdm import tqdm

import appletv
from page import blocked_qids
from properties import APPLE_TV_MOVIE_ID_PID, INSTANCE_OF_PID, ITUNES_MOVIE_ID_PID
from sparql import fetch_statements, sample_items, type_constraints


def main():
    """
    Find Wikidata items that are missing a iTunes movie ID (P6398) but have a
    Apple TV movie ID (P9586).

    Outputs QuickStatements CSV commands.
    """

    qids = sample_items(APPLE_TV_MOVIE_ID_PID, limit=1000)

    allowed_classes = type_constraints(ITUNES_MOVIE_ID_PID)
    results = fetch_statements(
        qids, [INSTANCE_OF_PID, ITUNES_MOVIE_ID_PID, APPLE_TV_MOVIE_ID_PID]
    )

    print("qid,P6398")
    for qid in tqdm(results):
        item = results[qid]

        if qid in blocked_qids():
            logging.debug(f"{qid} is blocked")
            continue

        if not item.get(INSTANCE_OF_PID) or item.get(ITUNES_MOVIE_ID_PID):
            continue

        instance_of = set([v for (_, v) in item[INSTANCE_OF_PID]])
        if instance_of.isdisjoint(allowed_classes):
            continue

        for (_statement, value) in item.get(APPLE_TV_MOVIE_ID_PID, []):
            id = appletv.id(value)
            movie = appletv.movie(id)
            if movie and movie["itunes_id"]:
                print(f'{qid},"""{movie["itunes_id"]}"""')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
