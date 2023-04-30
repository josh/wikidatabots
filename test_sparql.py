# pyright: strict

import polars as pl

from sparql import fetch_property_statements, sparql, sparql_batch


def _extract_qid(name: str = "item") -> pl.Expr:
    return pl.col(name).str.extract(r"^http://www.wikidata.org/entity/(Q\d+)$", 1)


def test_sparql() -> None:
    lf = sparql(
        """
        SELECT ?item ?itemLabel WHERE {
          ?item wdt:P31 wd:Q146.
          SERVICE wikibase:label {
            bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
          }
        }
        LIMIT 10
        """,
        columns=["item", "itemLabel"],
    ).with_columns(_extract_qid("item").alias("qid"))
    assert lf.schema == {"item": pl.Utf8, "itemLabel": pl.Utf8, "qid": pl.Utf8}
    df = lf.collect()
    assert len(df) == 10


def test_sparql_batch() -> None:
    ldf = (
        pl.LazyFrame({"pid": ["P4947", "P4983", "P4985"]})
        .with_columns(
            pl.format("SELECT ?n WHERE { wd:{} wdt:P4876 ?n. }", pl.col("pid"))
            .pipe(sparql_batch, columns=["n"])
            .alias("results"),
        )
        .explode("results")
        .with_columns(
            pl.col("results").struct.field("n").alias("n"),
        )
        .drop("results")
    )
    assert ldf.schema == {"pid": pl.Utf8, "n": pl.Utf8}
    df = ldf.collect()
    assert len(df) == 3


def test_fetch_property_statements() -> None:
    ldf = fetch_property_statements(pid="P9750")
    assert ldf.schema == {"subject": pl.Utf8, "object": pl.Utf8}
    df = ldf.collect()
    assert len(df) > 1
