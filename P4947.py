from tqdm import tqdm

import tmdb
from sparql import fetch_statements, sample_items, type_constraints


def main():
    """
    Find Wikidata items that are missing a TMDb movie ID (P4947) but have a
    IMDb ID (P345). Attempt to look up the movie by IMDb ID via the TMDb API.
    If there's a match, create a new statement.

    Outputs QuickStatements CSV commands.
    """

    allowed_classes = type_constraints("P4947")

    qids = sample_items("P345", limit=2500)

    results = fetch_statements(qids, ["P31", "P345", "P4947"])

    print("qid,P4947")
    for qid in tqdm(results):
        item = results[qid]

        if not item.get("P31") or item.get("P4947"):
            continue

        instance_of = set([v for (_, v) in item["P31"]])
        if instance_of.isdisjoint(allowed_classes):
            continue

        for (statement, value) in item.get("P345", []):
            movie = tmdb.find(id=value, source="imdb_id", type="movie")
            if movie:
                print('{},"""{}"""'.format(qid, movie["id"]))


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
