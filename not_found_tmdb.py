# pyright: basic

import logging
import sys

import pandas as pd

import tmdb
from constants import (
    REASON_FOR_DEPRECATED_RANK_PID,
    TMDB_MOVIE_ID_PID,
    TMDB_PERSON_ID_PID,
    TMDB_TV_SERIES_ID_PID,
    WITHDRAWN_IDENTIFIER_VALUE_QID,
)
from sparql import sparql_csv

PROPERTY_MAP: dict[tmdb.ObjectType, str] = {
    "movie": TMDB_MOVIE_ID_PID,
    "tv": TMDB_TV_SERIES_ID_PID,
    "person": TMDB_PERSON_ID_PID,
}


def main(tmdb_type: tmdb.ObjectType):
    # TODO: Precompute latest changes
    changes_uri = f"s3://wikidatabots/tmdb/{tmdb_type}/changes.arrow"
    changes_df = (
        pd.read_feather(changes_uri, columns=["id", "adult"])
        .drop_duplicates(subset=["id"], keep="last")
        .set_index("id")
        .sort_index()
    )
    changes_df["has_changes"] = True

    query = """
    SELECT ?statement ?value WHERE {
      ?statement ps:P0000 ?value.
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
      FILTER(xsd:integer(?value))
    }
    """
    query = query.replace("P0000", PROPERTY_MAP[tmdb_type])
    df = pd.read_csv(sparql_csv(query))

    df = df.join(changes_df, on="value", how="left", rsuffix="_changes")
    df = df[df["adult"].isna() & df["has_changes"]]

    if df.empty:
        return

    logging.info(f"Verifying {len(df)} {tmdb_type} IDs against API")
    df = df.assign(tmdb_exists=lambda row: bool(tmdb.object(row.value, type=tmdb_type)))
    df = df[~df["tmdb_exists"]]
    logging.info(f"{len(df)} {tmdb_type} IDs are not found in API")

    if df.empty:
        return

    for statement in df["statement"]:
        print(
            f"<{statement}> "
            f"wikibase:rank wikibase:DeprecatedRank ; "
            f"pq:{REASON_FOR_DEPRECATED_RANK_PID} "
            f"wd:{WITHDRAWN_IDENTIFIER_VALUE_QID} ; "
            f'wikidatabots:editSummary "Deprecate removed TMDB {tmdb_type} ID" . '
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    tmdb_type = sys.argv[1]
    assert tmdb_type in PROPERTY_MAP
    main(tmdb_type=tmdb_type)
