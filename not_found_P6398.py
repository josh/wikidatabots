import itunes
import sparql
from items import WITHDRAWN_IDENTIFIER_VALUE_QID
from page import page_qids
from sparql import sample_items
from utils import tryint


def main():
    (id, obj) = next(itunes.batch_lookup([567661493]))
    assert id and obj

    qids = sample_items("P6398", limit=10000)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P6398")

    results = sparql.fetch_statements(qids, ["P6398"])

    itunes_ids: dict[int, str] = {}

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get("P6398", []):
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
