# pyright: strict

import logging

import polars as pl

from sparql import sparql_df
from tmdb_etl import TMDB_TYPE, tmdb_exists


def main(tmdb_type: TMDB_TYPE):
    rdf_statement = pl.format(
        "<{}> wikibase:rank wikibase:DeprecatedRank ; pq:P2241 wd:Q21441764 ; "
        'wikidatabots:editSummary "{}" .',
        pl.col("statement"),
        pl.lit(f"Deprecate removed TMDB {tmdb_type} ID"),
    )

    changes_df = pl.scan_ipc(
        f"s3://wikidatabots/tmdb/{tmdb_type}/latest_changes.arrow",
        storage_options={"anon": True},
    )

    query = """
    SELECT ?statement ?id WHERE {
      ?statement ps:P0000 ?id.
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
      FILTER(xsd:integer(?id))
    }
    """
    props = {"movie": "P4947", "tv": "P4983", "person": "P4985"}
    query = query.replace("P0000", props[tmdb_type])
    df = sparql_df(query, dtypes={"statement": pl.Utf8, "id": pl.UInt32})

    df = (
        df.join(changes_df, on="id", how="left")
        .filter(pl.col("adult").is_null() & pl.col("has_changes"))
        .rename({"id": "tmdb_id"})
        .filter(tmdb_exists(tmdb_type).is_not())
        .select(rdf_statement)
    )

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    main("movie")
    main("tv")
    main("person")
