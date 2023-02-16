import logging

import polars as pl

from sparql import sparql_df
from tmdb_etl import EXTRACT_IMDB_TITLE_NUMERIC_ID, tmdb_find_by_external_id

QID_IMDB_ID_QUERY = """
SELECT ?item ?imdb_id WHERE {
    ?item wdt:P345 ?imdb_id.
    VALUES ?classes {
        wd:Q15416
    }
    ?item (wdt:P31/(wdt:P279*)) ?classes.
    OPTIONAL { ?item p:P4983 ?tmdb_id. }
    FILTER(!(BOUND(?tmdb_id)))
}
"""


def main_imdb():
    tmdb_df = (
        pl.scan_ipc("s3://wikidatabots/tmdb/tv/external_ids.arrow")
        .select(["id", "imdb_numeric_id"])
        .rename({"id": "tmdb_id"})
        .drop_nulls()
        .unique(subset=["imdb_numeric_id"])
    )

    wd_df = (
        sparql_df(QID_IMDB_ID_QUERY, columns=["item", "imdb_id"])
        .with_columns(EXTRACT_IMDB_TITLE_NUMERIC_ID)
        .drop_nulls()
    )

    joined_df = (
        wd_df.join(tmdb_df, on="imdb_numeric_id", how="left")
        .drop_nulls()
        .select(["item", "imdb_id"])
        .pipe(tmdb_find_by_external_id, tmdb_type="tv", external_id_type="imdb_id")
        .select(["item", "tmdb_id"])
    )

    edit_summary = "Add TMDb TV series ID claim via associated IMDb ID"
    for item, tmdb_id in joined_df.collect().iter_rows():
        print(
            f'<{item}> wdt:P4983 "{tmdb_id}" ; '
            f'wikidatabots:editSummary "{edit_summary}" .'
        )


QID_TVDB_ID_QUERY = """
SELECT ?item ?tvdb_id WHERE {
    ?item wdt:P4835 ?tvdb_id.
    VALUES ?classes {
        wd:Q15416
    }
    ?item (wdt:P31/(wdt:P279*)) ?classes.
    FILTER(xsd:integer(?tvdb_id))
    OPTIONAL { ?item p:P4983 ?tmdb_id. }
    FILTER(!(BOUND(?tmdb_id)))
}
"""


def main_tvdb():
    tmdb_df = (
        pl.scan_ipc("s3://wikidatabots/tmdb/tv/external_ids.arrow")
        .select(["id", "tvdb_id"])
        .rename({"id": "tmdb_id"})
        .drop_nulls()
        .unique(subset=["tvdb_id"])
    )

    wd_df = sparql_df(
        QID_TVDB_ID_QUERY,
        dtypes={"item": pl.Utf8, "tvdb_id": pl.UInt32},
    ).drop_nulls()

    joined_df = (
        wd_df.join(tmdb_df, on="tvdb_id", how="left")
        .drop_nulls()
        .select(["item", "tvdb_id"])
        .pipe(tmdb_find_by_external_id, tmdb_type="tv", external_id_type="tvdb_id")
        .select(["item", "tmdb_id"])
        .drop_nulls()
    )

    edit_summary = "Add TMDb TV series ID claim via associated TheTVDB.com series ID"
    for item, tmdb_id in joined_df.collect().iter_rows():
        print(
            f'<{item}> wdt:P4983 "{tmdb_id}" ; '
            f'wikidatabots:editSummary "{edit_summary}" .'
        )


def main():
    main_tvdb()
    main_imdb()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
