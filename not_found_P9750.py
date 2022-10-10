import re

import appletv
import sparql
from items import WITHDRAWN_IDENTIFIER_VALUE_QID
from page import page_qids
from properties import APPLE_TV_EPISODE_ID_PID
from sparql import sample_items

MATCHER = re.compile("^umc.cmc.[a-z0-9]{22,25}$")


def main():
    assert not appletv.all_not_found(
        type="episode", id="umc.cmc.1488ez4dp942etebq3p85k1np"
    )

    qids = sample_items(APPLE_TV_EPISODE_ID_PID, limit=250)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P9750")

    results = sparql.fetch_statements(qids, [APPLE_TV_EPISODE_ID_PID])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get(APPLE_TV_EPISODE_ID_PID, []):
            if not MATCHER.match(value):
                continue

            if appletv.all_not_found(type="episode", id=value):
                print(f"{statement},{WITHDRAWN_IDENTIFIER_VALUE_QID}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
