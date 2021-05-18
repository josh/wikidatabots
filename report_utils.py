import math
import os
import sys

import sparql
from page_extract import page_qids

if "WIKIDATA_USERNAME" in os.environ:
    WIKIDATA_USERNAME = os.environ["WIKIDATA_USERNAME"]
else:
    print("WARN: WIKIDATA_USERNAME unset", file=sys.stderr)


def sample_qids(property, count=1000, username=WIKIDATA_USERNAME):
    limit = math.floor(count / 3)

    qids = set()
    qids |= sparql.sample_items(property, type="random", limit=limit)
    qids |= sparql.sample_items(property, type="created", limit=limit)
    qids |= sparql.sample_items(property, type="updated", limit=limit)

    if username:
        qids |= page_qids("User:{}/Maintenance_reports/{}".format(username, property))

    return qids
