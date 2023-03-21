import logging

import polars as pl

from polars_utils import assert_expression
from sparql import sparql_df

_PIDS: list[str] = [
    "P4947",
    "P4983",
    "P4985",
    "P6398",
    "P9586",
    "P9750",
    "P9751",
    "P11460",
]

_CONSTRAINT_INSTANCE_OF_QUERY = """
SELECT DISTINCT ?class ?classLabel WHERE {
  wd:P0000 p:P2302 [
    ps:P2302 wd:Q21503250;
    pq:P2309 wd:Q21503252;
    pq:P2308 ?class
  ].
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
"""

_CONSTRAINT_SUBCLASS_OF_QUERY = """
SELECT DISTINCT ?class ?classLabel WHERE {
  wd:P0000 p:P2302 [
    ps:P2302 wd:Q21503250;
    pq:P2309 wd:Q30208840;
    pq:P2308 ?class_
  ].
  ?class (wdt:P279*) ?class_.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
"""

_CONSTRAINT_QUERY_SCHEMA: dict[str, pl.PolarsDataType] = {
    "class": pl.Utf8,
    "classLabel": pl.Utf8,
}


def _fetch_property_class_constraints(pid: str) -> pl.LazyFrame:
    numeric_pid = int(pid[1:])
    query1 = _CONSTRAINT_INSTANCE_OF_QUERY.replace("P0000", pid)
    query2 = _CONSTRAINT_SUBCLASS_OF_QUERY.replace("P0000", pid)

    return (
        pl.concat(
            [
                sparql_df(query1, schema=_CONSTRAINT_QUERY_SCHEMA),
                sparql_df(query2, schema=_CONSTRAINT_QUERY_SCHEMA),
            ],
            parallel=False,  # BUG: parallel caching is broken
        )
        .unique(subset=["class"])
        .rename({"class": "class_uri", "classLabel": "class_label"})
        .with_columns(
            pl.col("class_uri")
            .str.extract(r"^http://www.wikidata.org/entity/Q(\d+)$", 1)
            .cast(pl.UInt32)
            .alias("class_numeric_qid")
        )
        .drop_nulls("class_numeric_qid")
        .sort("class_numeric_qid")
        .with_columns(
            pl.format("Q{}", pl.col("class_numeric_qid"))
            .cast(pl.Categorical)
            .alias("class_qid")
        )
        .with_columns(
            pl.lit(pid).cast(pl.Categorical).alias("pid"),
            pl.lit(numeric_pid).alias("numeric_pid"),
        )
        .select(
            "numeric_pid",
            "pid",
            "class_numeric_qid",
            "class_qid",
            "class_label",
        )
        .pipe(
            assert_expression,
            pl.col("class_numeric_qid").is_not_null()
            & pl.col("class_numeric_qid").is_unique(),
        )
    )


def _fetch_all_property_class_constraints() -> pl.LazyFrame:
    return pl.concat(
        [_fetch_property_class_constraints(pid) for pid in _PIDS],
        parallel=False,  # BUG: parallel caching is broken
    )


def _main() -> None:
    pl.toggle_string_cache(True)

    df = _fetch_all_property_class_constraints()
    df.collect().write_ipc("property_class_constraints.arrow", compression="lz4")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main()
