import itunes
import sparql
from items import WITHDRAWN_IDENTIFIER_VALUE_QID
from page import page_qids
from properties import ITUNES_MOVIE_ID_PID
from sparql import sample_items
from utils import tryint


def main():
    (id, obj) = next(itunes.batch_lookup([567661493]))
    assert id and obj

    qids = sample_items(ITUNES_MOVIE_ID_PID, limit=10000)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P6398")

    results = sparql.fetch_statements(qids, [ITUNES_MOVIE_ID_PID])

    itunes_ids: dict[int, str] = {}

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get(ITUNES_MOVIE_ID_PID, []):
            id = tryint(value)
            if id:
                itunes_ids[id] = statement

    for (id, obj) in itunes.batch_lookup(itunes_ids.keys()):
        if not obj and itunes.all_not_found(id):
            print(f"{itunes_ids[id]},{WITHDRAWN_IDENTIFIER_VALUE_QID}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
