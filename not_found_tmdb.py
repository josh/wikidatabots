# pyright: basic

import logging

import pandas as pd
from rdflib import URIRef

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
    export_uri = f"s3://wikidatabots/tmdb/{tmdb_type}/export.arrow"
    export_df = pd.read_feather(export_uri, columns=["id", "in_export"])

    query = """
    SELECT ?statement ?value ?rank WHERE {
      ?statement ps:P0000 ?value.
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
      FILTER(xsd:integer(?value))
    }
    """
    query = query.replace("P0000", PROPERTY_MAP[tmdb_type])
    df = pd.read_csv(sparql_csv(query))

    df = df.join(export_df, on="value", how="left")
    df = df[df["in_export"] != True]  # noqa: E712

    edit_summary = f"Deprecate removed TMDB {tmdb_type} ID"

    for row in df.itertuples():
        statement = URIRef(row.statement)
        id = int(row.value)

        if not tmdb.object(id, type=tmdb_type):
            print(
                f"{statement.n3()} "
                f"wikibase:rank wikibase:DeprecatedRank ; "
                f"pq:{REASON_FOR_DEPRECATED_RANK_PID} "
                f"wd:{WITHDRAWN_IDENTIFIER_VALUE_QID} ; "
                f'wikidatabots:editSummary "{edit_summary}" . '
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    main(tmdb_type="movie")
    main(tmdb_type="tv")
    main(tmdb_type="person")
