# pyright: basic

import logging
from typing import TypedDict

import pyarrow.feather as feather
from pyarrow import fs

import tmdb
from constants import (
    REASON_FOR_DEPRECATED_RANK_PID,
    TMDB_MOVIE_ID_PID,
    TMDB_PERSON_ID_PID,
    TMDB_TV_SERIES_ID_PID,
    WITHDRAWN_IDENTIFIER_VALUE_QID,
)
from sparql import sparql
from utils import tryint
from wikidata import PID, WDSURIRef

PROPERTY_MAP: dict[tmdb.ObjectType, PID] = {
    "movie": TMDB_MOVIE_ID_PID,
    "tv": TMDB_TV_SERIES_ID_PID,
    "person": TMDB_PERSON_ID_PID,
}


def main(type: tmdb.ObjectType):
    s3 = fs.S3FileSystem(region="us-east-1")
    logging.info(f"Reading feather {type} mask")
    f = s3.open_input_file(f"wikidatabots/tmdb/{type}/mask.arrow")
    table = feather.read_table(f)
    is_null_col = table["null"]

    query = """
    SELECT ?statement ?value WHERE {
      ?statement ps:P0000 ?value.
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
    }
    """
    query = query.replace("P0000", PROPERTY_MAP[type])

    Result = TypedDict("Result", statement=WDSURIRef, value=str)
    results: list[Result] = sparql(query)

    edit_summary = f"Deprecate removed TMDB {type} ID"

    for result in results:
        statement = result["statement"]
        id = tryint(result["value"])
        if not id:
            continue

        if id < table.num_rows and is_null_col[id].as_py() is False:
            continue

        if not tmdb.object(id, type=type):
            print(
                f"{statement.n3()} "
                f"wikibase:rank wikibase:DeprecatedRank ; "
                f"pq:{REASON_FOR_DEPRECATED_RANK_PID} "
                f"wd:{WITHDRAWN_IDENTIFIER_VALUE_QID} ; "
                f'wikidatabots:editSummary "{edit_summary}" . '
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    main(type="movie")
    main(type="tv")
    main(type="person")
