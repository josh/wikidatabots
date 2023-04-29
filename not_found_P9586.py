# pyright: strict

import polars as pl

from appletv_etl import not_found
from polars_utils import sample
from sparql import sparql_df

_QUERY = """
SELECT ?statement ?id WHERE {
  ?statement ps:P9586 ?id.
  ?statement wikibase:rank ?rank.
  FILTER(?rank != wikibase:DeprecatedRank)
}
"""

_RDF_STATEMENT = pl.format(
    "<{}> wikibase:rank wikibase:DeprecatedRank ; pq:P2241 wd:Q21441764 ; "
    'wikidatabots:editSummary "Deprecate Apple TV movie ID delisted from store" .',
    pl.col("statement"),
).alias("rdf_statement")


def find_movie_not_found() -> pl.LazyFrame:
    return (
        sparql_df(_QUERY, columns=["statement", "id"])
        .with_columns(
            pl.col("id").str.extract("^(umc.cmc.[a-z0-9]{22,25})$").alias("id"),
        )
        .drop_nulls()
        .select("statement", "id")
        .pipe(sample, n=25)
        .pipe(not_found, type="movie")
        .filter(pl.col("all_not_found"))
        .select(_RDF_STATEMENT)
    )


def main() -> None:
    df = pl.concat(
        [
            find_movie_not_found(),
        ]
    )

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
