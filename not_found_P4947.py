from tqdm import tqdm

import tmdb
from constants import WITHDRAWN_IDENTIFIER_VALUE_QID
from sparql import sparql
from utils import tryint


def main():
    assert tmdb.object(140607, type="movie")

    query = """
    SELECT ?statement ?value WHERE {
      ?statement ps:P4947 ?value.
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
    }
    """

    for result in tqdm(sparql(query)):
        statement = result["statement"]
        assert type(statement) is str

        id = tryint(result["value"])

        if id and not tmdb.object(id, type="movie"):
            print(f"{statement},{WITHDRAWN_IDENTIFIER_VALUE_QID}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
