import re

import appletv
import sparql
from report_utils import sample_qids

MATCHER = re.compile("^umc.cmc.[a-z0-9]{22,25}$")


def main():
    qids = sample_qids("P9586", count=100)
    results = sparql.fetch_statements(qids, ["P9586"])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get("P9586", []):
            if not MATCHER.match(value):
                continue

            if not appletv.movie(value):
                print("{},Q21441764".format(statement))


if __name__ == "__main__":
    main()
