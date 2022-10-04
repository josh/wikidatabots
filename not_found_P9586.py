import re

import appletv
import sparql
from page import page_qids
from sparql import sample_items

MATCHER = re.compile("^umc.cmc.[a-z0-9]{22,25}$")


def main():
    assert not appletv.all_not_found(
        type="movie", id="umc.cmc.o5z5ztufuu3uv8lx7m0jcega"
    )

    qids = sample_items("P9586", limit=250)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P9586")

    results = sparql.fetch_statements(qids, ["P9586"])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get("P9586", []):
            if not MATCHER.match(value):
                continue

            if appletv.all_not_found(type="movie", id=value):
                print(f"{statement},Q21441764")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
