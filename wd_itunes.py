# pyright: strict

import polars as pl

from appletv_etl import appletv_to_itunes_series
from polars_utils import limit, print_rdf_statements
from sparql import sparql

_ADD_RDF_STATEMENT = pl.format(
    '<{}> wdt:P6398 "{}" ; '
    'wikidatabots:editSummary "Add iTunes movie ID via Apple TV movie ID" . ',
    pl.col("item"),
    pl.col("itunes_id"),
).alias("rdf_statement")


_APPLETV_QUERY = """
SELECT ?item ?appletv_id WHERE {
  ?item wdt:P9586 ?appletv_id.

  # iTunes movie ID subject type constraints
  VALUES ?class {
    wd:Q11424
    wd:Q1261214
  }
  ?item (wdt:P31/(wdt:P279*)) ?class.

  OPTIONAL { ?item wdt:P6398 ?itunes_id. }
  FILTER(!(BOUND(?itunes_id)))
}
"""

_LOOKUP_LIMIT = 100


def _itunes_from_appletv_ids(itunes_df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        sparql(_APPLETV_QUERY, columns=["item", "appletv_id"])
        .pipe(limit, _LOOKUP_LIMIT, desc="appletv_ids")
        .with_columns(
            pl.col("appletv_id")
            .map(appletv_to_itunes_series, return_dtype=pl.UInt64)
            .alias("itunes_id")
        )
        .join(itunes_df, left_on="itunes_id", right_on="id", how="left")
        .filter(pl.col("any_country"))
        .select(_ADD_RDF_STATEMENT)
    )


def main() -> None:
    itunes_df = (
        pl.scan_parquet(
            "s3://wikidatabots/itunes.parquet",
            storage_options={"anon": True},
        )
        .select(["id", "any_country"])
        .cache()
    )

    pl.concat(
        [
            _itunes_from_appletv_ids(itunes_df),
        ]
    ).pipe(print_rdf_statements)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
