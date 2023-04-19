# pyright: strict

import polars as pl

from itunes_etl import lookup_itunes_id
from sparql import sparql_df

_EDIT_SUMMARY = "Deprecate iTunes movie ID delisted from store"


def _deprecated_rdf_statement() -> pl.Expr:
    return pl.format(
        "<{}> wikibase:rank wikibase:DeprecatedRank ; "
        "pq:P2241 wd:Q21441764 ; "
        f'wikidatabots:editSummary "{_EDIT_SUMMARY}" . ',
        pl.col("statement"),
    ).alias("rdf_statement")


_QUERY = """
SELECT ?statement ?id WHERE {
  ?statement ps:P6398 ?id;
    wikibase:rank ?rank.
  FILTER(?rank != wikibase:DeprecatedRank)
  FILTER(xsd:integer(?id))
}
"""


def _delisted_itunes_ids() -> pl.LazyFrame:
    df = pl.scan_parquet(
        "s3://wikidatabots/itunes.parquet", storage_options={"anon": True}
    ).select(["id", "any_country"])

    return (
        sparql_df(_QUERY, schema={"statement": pl.Utf8, "id": pl.UInt64})
        .join(df, on="id")
        .filter(pl.col("any_country").is_not())
        .with_columns(
            pl.col("id")
            .pipe(lookup_itunes_id, country="us")
            .struct.field("id")
            .is_not_null()
            .alias("any_country"),
        )
        .filter(pl.col("any_country").is_not())
        .select(_deprecated_rdf_statement())
    )


def main() -> None:
    df = _delisted_itunes_ids()

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
