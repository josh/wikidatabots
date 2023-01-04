# pyright: basic

from typing import Any, Literal

import pandas as pd

import tmdb
from constants import TMDB_TV_SERIES_ID_PID
from sparql import sparql_csv

import logging


def load_tmdb_external_ids_df(
    type: tmdb.ObjectType,
    source: tmdb.FindSource,
) -> pd.DataFrame:
    uri = f"s3://wikidatabots/tmdb/{type}/external_ids.arrow"
    tmdb_df = (
        pd.read_feather(uri, columns=["id", source])
        .rename(columns={"id": "tmdb_id"})
        .drop_duplicates(subset=source, keep=False)
        .set_index(source)
    )
    return tmdb_df


def load_wd_external_ids_df(
    source: tmdb.FindSource,
    source_pid: str,
    source_dtype: Literal["string", "UInt32"],
) -> pd.DataFrame:
    query = ""
    query += f"SELECT ?wd_uri ?{source} WHERE "
    query += "{\n"
    query += f"  ?wd_uri wdt:{source_pid} ?{source}.\n"
    if source_dtype == "UInt32":
        query += f"  FILTER(xsd:integer(?{source}))\n"

    query += """
      VALUES ?classes {
        wd:Q15416
      }
      ?wd_uri (wdt:P31/(wdt:P279*)) ?classes.
      OPTIONAL { ?wd_uri p:P4983 ?tmdb_id. }
      FILTER(!(BOUND(?tmdb_id)))
    }
    """
    data = sparql_csv(query)
    dtype: Any = {"wd_uri": "string", source: source_dtype}
    return pd.read_csv(data, dtype=dtype)


def merge_wd_tmdb_ids(
    tmdb_type: tmdb.ObjectType,
    tmdb_source: tmdb.FindSource,
    source_pid: str,
    source_dtype: Literal["string", "UInt32"],
    progress: bool,
) -> pd.DataFrame:
    tmdb_df = load_tmdb_external_ids_df(type=tmdb_type, source=tmdb_source)
    wd_df = load_wd_external_ids_df(
        source=tmdb_source,
        source_pid=source_pid,
        source_dtype=source_dtype,
    )

    joined_df = wd_df.join(tmdb_df, on=tmdb_source, how="left", rsuffix="_tmdb")
    joined_df = joined_df.dropna(subset=["tmdb_id"])

    tmdb_ids = tmdb.find_ids(
        joined_df[tmdb_source],
        source=tmdb_source,
        type=tmdb_type,
        progress=progress,
    )
    joined_df["tmdb_id"] = pd.Series(
        data=tmdb_ids,
        index=joined_df.index,
        dtype="UInt32",
    )
    joined_df = joined_df.dropna(subset=["tmdb_id"])

    return joined_df


def print_rdf_triples(df, pid: str, edit_summary):
    for row in df.itertuples():
        print(
            f"<{row.wd_uri}> "
            f'wdt:{pid} "{row.tmdb_id}" ; '
            f'wikidatabots:editSummary "{edit_summary}" .'
        )


def main():
    joined_df = merge_wd_tmdb_ids(
        tmdb_type="tv",
        tmdb_source="imdb_id",
        source_pid="P345",
        source_dtype="string",
        progress=True,
    )
    print_rdf_triples(
        joined_df,
        pid=TMDB_TV_SERIES_ID_PID,
        edit_summary="Add TMDb TV series ID claim via associated IMDb ID",
    )

    joined_df = merge_wd_tmdb_ids(
        tmdb_type="tv",
        tmdb_source="tvdb_id",
        source_pid="P4835",
        source_dtype="UInt32",
        progress=True,
    )
    print_rdf_triples(
        joined_df,
        pid=TMDB_TV_SERIES_ID_PID,
        edit_summary="Add TMDb TV series ID claim via associated TheTVDB.com series ID",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
