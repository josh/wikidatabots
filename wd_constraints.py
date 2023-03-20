import logging

import polars as pl

from sparql import sparql_df

_QUERY = """
SELECT DISTINCT ?property ?subclass WHERE {
  VALUES ?property {
    wd:P4947
    wd:P4983
    wd:P4985
    wd:P6398
    wd:P9586
    wd:P9750
    wd:P9751
    wd:P11460
  }

  ?property p:P2302 [
    ps:P2302 wd:Q21503250;
    pq:P2309 wd:Q21503252;
    pq:P2308 ?class
  ].

  ?subclass (wdt:P279*) ?class.
}
"""


def _generate_wd_constraints() -> pl.LazyFrame:
    return (
        sparql_df(_QUERY, columns=["property", "subclass"])
        .select(
            (
                pl.col("property")
                .str.extract(r"^http://www.wikidata.org/entity/P(\d+)$", 1)
                .cast(pl.UInt32)
                .alias("pid")
            ),
            (
                pl.col("subclass")
                .str.extract(r"^http://www.wikidata.org/entity/Q(\d+)$", 1)
                .cast(pl.UInt32)
                .alias("instance_of_qid")
            ),
        )
        .drop_nulls()
        .sort("pid", "instance_of_qid")
    )


def load_wd_constraints() -> pl.LazyFrame:
    return pl.scan_csv(
        "wd_constraints.csv",
        dtypes={"pid": pl.UInt32, "instance_of_qid": pl.UInt32},
    )


def _main() -> None:
    _generate_wd_constraints().collect().write_csv("wd_constraints.csv")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main()
