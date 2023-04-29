# pyright: strict

import appletv
import sparql
from constants import APPLE_TV_EPISODE_ID_PID, WITHDRAWN_IDENTIFIER_VALUE_QID
from sparql import sample_items
from timeout import iter_until_deadline
from wikidata import page_qids


def main():
    assert not appletv.all_not_found(
        type="episode", id=appletv.id("umc.cmc.1488ez4dp942etebq3p85k1np")
    )

    pid = APPLE_TV_EPISODE_ID_PID
    qids = sample_items(pid, limit=250)
    qids |= set(page_qids(f"Wikidata:Database reports/Constraint violations/{pid}"))

    results = sparql.fetch_statements(qids, [pid])

    for qid in iter_until_deadline(results):
        item = results[qid]

        for statement, value in item.get(pid, []):
            id = appletv.tryid(value)
            if not id:
                continue

            if appletv.all_not_found(type="episode", id=id):
                print(f"{statement},{WITHDRAWN_IDENTIFIER_VALUE_QID}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
