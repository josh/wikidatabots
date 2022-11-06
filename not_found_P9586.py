# pyright: strict

import appletv
import sparql
from constants import APPLE_TV_MOVIE_ID_PID, WITHDRAWN_IDENTIFIER_VALUE_QID
from page import page_qids
from sparql import sample_items


def main():
    assert not appletv.all_not_found(
        type="movie", id=appletv.id("umc.cmc.o5z5ztufuu3uv8lx7m0jcega")
    )

    qids = sample_items(APPLE_TV_MOVIE_ID_PID, limit=250)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P9586")

    results = sparql.fetch_statements(qids, [APPLE_TV_MOVIE_ID_PID])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get(APPLE_TV_MOVIE_ID_PID, []):
            id = appletv.tryid(value)
            if not id:
                continue

            if appletv.all_not_found(type="movie", id=id):
                print(f"{statement},{WITHDRAWN_IDENTIFIER_VALUE_QID}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
