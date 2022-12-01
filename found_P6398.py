# pyright: strict

from typing import TypedDict

from rdflib import URIRef

import itunes
import sparql
import wikidata
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

    qids: set[wikidata.QID] = set()
    for result in results:
        qids.add(wikidata.qid(result["item"]))

    statements = sparql.fetch_statements(qids, [ITUNES_MOVIE_ID_PID], deprecated=True)
    itunes_ids = extract_itunes_ids(statements)

    for (id, obj) in itunes.batch_lookup(itunes_ids.keys()):
        if obj:
            uri = itunes_ids[id]
            snak = "$".join(uri[41:].split("-", 1))
            print(f"{snak},{NORMAL_RANK_QID}")


def extract_itunes_ids(
    statements: dict[wikidata.QID, dict[wikidata.PID, list[tuple[URIRef, str]]]]
) -> dict[int, URIRef]:
    itunes_ids: dict[int, URIRef] = {}
    for item in statements.values():
        for (statement, value) in item.get(ITUNES_MOVIE_ID_PID, []):
            if id := tryint(value):
                itunes_ids[id] = statement
    return itunes_ids


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
