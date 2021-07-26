import re

import appletv
import sparql
from page import page_qids
from sparql import sample_items

MATCHER = re.compile("^umc.cmc.[a-z0-9]{22,25}$")


def main():
    assert not appletv.all_not_found(type="show", id="umc.cmc.vtoh0mn0xn7t3c643xqonfzy")

    qids = sample_items("P9751", limit=250)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P9751")

    results = sparql.fetch_statements(qids, ["P9751"])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get("P9751", []):
            if not MATCHER.match(value):
                continue

            if appletv.all_not_found(type="show", id=value):
                print("{},Q21441764".format(statement))


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
