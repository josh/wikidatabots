# pyright: basic

from typing import TypedDict

import pyarrow.feather as feather
from pyarrow import fs

import tmdb
from constants import REASON_FOR_DEPRECATED_RANK_PID, WITHDRAWN_IDENTIFIER_VALUE_QID
from sparql import sparql
from utils import tryint
from wikidata import WDSURIRef


def main():
    s3 = fs.S3FileSystem(region="us-east-1")
    f = s3.open_input_file("wikidatabots/tmdb/tv/mask.arrow")
    table = feather.read_table(f)
    is_null_col = table["null"]

    query = """
    SELECT ?statement ?value WHERE {
      ?statement ps:P4983 ?value.
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
    }
    """
    Result = TypedDict("Result", statement=WDSURIRef, value=str)
    results: list[Result] = sparql(query)

    edit_summary = "Deprecate removed TMDB TV series ID"

    for result in results:
        statement = result["statement"]
        id = tryint(result["value"])
        if not id:
            continue

        if id < table.num_rows and is_null_col[id].as_py() is False:
            continue

        if not tmdb.object(id, type="tv"):
            print(
                f"{statement.n3()} "
                f"wikibase:rank wikibase:DeprecatedRank ; "
                f"pq:{REASON_FOR_DEPRECATED_RANK_PID} "
                f"wd:{WITHDRAWN_IDENTIFIER_VALUE_QID} ; "
                f'wikidatabots:editSummary "{edit_summary}" . '
            )


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
