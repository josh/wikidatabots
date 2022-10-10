import re

import appletv
import sparql
from items import WITHDRAWN_IDENTIFIER_VALUE_QID
from page import page_qids
from properties import APPLE_TV_SHOW_ID_PID
from sparql import sample_items

MATCHER = re.compile("^umc.cmc.[a-z0-9]{22,25}$")


def main():
    assert not appletv.all_not_found(type="show", id="umc.cmc.vtoh0mn0xn7t3c643xqonfzy")

    qids = sample_items(APPLE_TV_SHOW_ID_PID, limit=250)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P9751")

    results = sparql.fetch_statements(qids, [APPLE_TV_SHOW_ID_PID])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get(APPLE_TV_SHOW_ID_PID, []):
            if not MATCHER.match(value):
                continue

            if appletv.all_not_found(type="show", id=value):
                print(f"{statement},{WITHDRAWN_IDENTIFIER_VALUE_QID}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
