# pyright: strict

from typing import TypedDict

import tmdb
from constants import WITHDRAWN_IDENTIFIER_VALUE_QID
from sparql import sparql
from timeout import iter_until_deadline
from utils import tryint


def main():
    assert tmdb.object(1399, type="tv")

    query = """
    SELECT ?statement ?value WHERE {
      ?statement ps:P4983 ?value.
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
    }
    """
    Result = TypedDict("Result", statement=str, value=str)
    results: list[Result] = sparql(query)

    for result in iter_until_deadline(results):
        statement = result["statement"]
        id = tryint(result["value"])

        if id and not tmdb.object(id, type="tv"):
            print(f"{statement},{WITHDRAWN_IDENTIFIER_VALUE_QID}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
