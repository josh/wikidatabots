# pyright: strict

import logging

import polars as pl

from polars_utils import update_parquet
from sparql import sparql_batch

_PIDS: list[str] = [
    "P345",
    "P4947",
    "P4983",
    "P4985",
    "P6398",
    "P9586",
    "P9750",
    "P9751",
    "P11460",
]

_CONSTRAINT_QUERY = """
SELECT DISTINCT ?class WHERE {
  VALUES ?property { wd:{} }
  {
    ?property p:P2302 [
      ps:P2302 wd:Q21503250;
      pq:P2309 wd:Q21503252;
      pq:P2308 ?class
    ].
  } UNION {
    ?property p:P2302 [
      ps:P2302 wd:Q21503250;
      pq:P2309 wd:Q30208840;
      pq:P2308 ?class_
    ].
    ?class (wdt:P279*) ?class_.
  }
}
"""


def fetch_property_class_constraints(pids: list[str]) -> pl.LazyFrame:
    return (
        pl.LazyFrame({"pid": pids})
        .with_columns(
            pl.col("pid").cast(pl.Categorical),
            pl.col("pid").str.replace("P", "").cast(pl.UInt32).alias("numeric_pid"),
        )
        .with_columns(
            pl.format(_CONSTRAINT_QUERY, pl.col("pid"))
            .pipe(sparql_batch, columns=["class"])
            .alias("results")
        )
        .explode("results")
        .with_columns(
            pl.col("results").struct.field("class").alias("class_uri"),
        )
        .drop("results")
        .with_columns(
            pl.col("class_uri")
            .str.extract(r"^http://www.wikidata.org/entity/Q(\d+)$", 1)
            .cast(pl.UInt32)
            .alias("class_numeric_qid")
        )
        .drop_nulls("class_numeric_qid")
        .with_columns(
            pl.format("Q{}", pl.col("class_numeric_qid"))
            .cast(pl.Categorical)
            .alias("class_qid")
        )
        .unique(["numeric_pid", "class_numeric_qid"])
        .sort(["numeric_pid", "class_numeric_qid"])
        .select(
            pl.format("{}-{}", pl.col("pid"), pl.col("class_qid")).alias("key"),
            "numeric_pid",
            "pid",
            "class_numeric_qid",
            "class_qid",
        )
    )


def _main() -> None:
    pl.enable_string_cache(True)

    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        return fetch_property_class_constraints(_PIDS)

    update_parquet("property_class_constraints.parquet", update, key="key")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main()
