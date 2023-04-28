# pyright: strict

import polars as pl

from appletv_etl import appletv_to_itunes_series
from itunes_etl import itunes_id_redirects_to_apple_tv, lookup_itunes_id
from polars_utils import limit
from sparql import sparql_df

_STATEMENT_LIMIT = (100, 10_000)

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
        sparql_df(_APPLETV_QUERY, columns=["item", "appletv_id"])
        .pipe(limit, soft=_LOOKUP_LIMIT, desc="appletv_ids")
        .with_columns(
            pl.col("appletv_id")
            .map(appletv_to_itunes_series, return_dtype=pl.UInt64)
            .alias("itunes_id")
        )
        .join(itunes_df, left_on="itunes_id", right_on="id", how="left")
        .filter(pl.col("any_country"))
        .select(_ADD_RDF_STATEMENT)
    )


_DEPRECATED_QUERY = """
SELECT ?statement ?id WHERE {
  ?statement ps:P6398 ?id;
    wikibase:rank ?rank.
  FILTER(?rank != wikibase:DeprecatedRank)
  FILTER(xsd:integer(?id))
}
"""

_DEPRECATE_RDF_STATEMENT = pl.format(
    "<{}> wikibase:rank wikibase:DeprecatedRank ; "
    "pq:P2241 wd:Q21441764 ; "
    'wikidatabots:editSummary "Deprecate iTunes movie ID delisted from store" . ',
    pl.col("statement"),
).alias("rdf_statement")


def xxx_delisted_itunes_ids(itunes_df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        sparql_df(_DEPRECATED_QUERY, schema={"statement": pl.Utf8, "id": pl.UInt64})
        .join(itunes_df, on="id")
        .filter(pl.col("any_country").is_not())
        .with_columns(
            pl.col("id")
            .pipe(lookup_itunes_id, country="us")
            .struct.field("id")
            .is_not_null()
            .alias("any_country"),
        )
        .filter(pl.col("any_country").is_not())
        .select(_DEPRECATE_RDF_STATEMENT)
    )


_UNDEPRECATED_QUERY = """
SELECT ?statement ?id WHERE {
  ?statement ps:P6398 ?id;
    pq:P2241 wd:Q21441764;
    wikibase:rank ?rank.
  FILTER(?rank = wikibase:DeprecatedRank)
  FILTER(xsd:integer(?id))
}
"""

_UNDEPRECATED_RDF_STATEMENT = pl.format(
    "<{}> wikibase:rank wikibase:NormalRank ; pq:P2241 [] ; . ",
    pl.col("statement"),
).alias("rdf_statement")


def xxx_relisted_itunes_ids(itunes_df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        sparql_df(_UNDEPRECATED_QUERY, schema={"statement": pl.Utf8, "id": pl.UInt64})
        .join(itunes_df, on="id")
        .with_columns(
            pl.col("id")
            .pipe(itunes_id_redirects_to_apple_tv)
            .alias("redirects_to_tv_apple")
        )
        .filter(pl.col("redirects_to_tv_apple"))
        .select(_UNDEPRECATED_RDF_STATEMENT)
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

    df = pl.concat(
        [
            _itunes_from_appletv_ids(itunes_df),
            # _delisted_itunes_ids(itunes_df),
        ]
    ).pipe(limit, _STATEMENT_LIMIT, desc="rdf_statements")

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
