# pyright: strict

from typing import TypedDict

import itunes
import sparql
from constants import ITUNES_MOVIE_ID_PID, NORMAL_RANK_QID
from utils import tryint


def main():
    (id, obj) = next(itunes.batch_lookup([567661493]))
    assert id and obj

    query = """
    SELECT ?item WHERE {
    ?item p:P6398 ?statement.
    ?statement ps:P6398 ?value;
        wikibase:rank ?rank;
        pq:P2241 wd:Q21441764.
    FILTER(?rank = wikibase:DeprecatedRank)
    }
    """
    Result = TypedDict("Result", item=str)
    results: list[Result] = sparql.sparql(query)

    qids: set[str] = set()
    for result in results:
        qids.add(result["item"])

    statements = sparql.fetch_statements(qids, [ITUNES_MOVIE_ID_PID], deprecated=True)
    itunes_ids = extract_itunes_ids(statements)

    for (id, obj) in itunes.batch_lookup(itunes_ids.keys()):
        if obj:
            print(f"{itunes_ids[id]},{NORMAL_RANK_QID}")


def extract_itunes_ids(
    statements: dict[str, dict[str, list[tuple[str, str]]]]
) -> dict[int, str]:
    itunes_ids: dict[int, str] = {}
    for item in statements.values():
        for (statement, value) in item.get(ITUNES_MOVIE_ID_PID, []):
            id = tryint(value)
            if id:
                itunes_ids[id] = statement
    return itunes_ids


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
