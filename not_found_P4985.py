from tqdm import tqdm

import tmdb
from sparql import sparql


def main():
    assert tmdb.object(31, type="person")

    query = """
    SELECT ?statement ?value WHERE {
      ?statement ps:P4985 ?value.
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
    }
    """

    for result in tqdm(sparql(query)):
        statement = result["statement"]

        try:
            id = int(result["value"])
        except ValueError:
            continue

        if not tmdb.object(id, type="person"):
            print("{},Q21441764".format(statement))


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
