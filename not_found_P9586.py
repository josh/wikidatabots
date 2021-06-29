import re

import appletv
import sparql
from report_utils import sample_qids

MATCHER = re.compile("^umc.cmc.[a-z0-9]{22,25}$")


def main():
    assert not appletv.not_found("umc.cmc.o5z5ztufuu3uv8lx7m0jcega")

    qids = sample_qids("P9586", count=250)
    results = sparql.fetch_statements(qids, ["P9586"])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get("P9586", []):
            if not MATCHER.match(value):
                continue

            url = "https://tv.apple.com/us/movie/{}".format(value)
            if appletv.not_found(url):
                print("{},Q21441764".format(statement))


if __name__ == "__main__":
    main()
