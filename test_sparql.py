# pyright: strict

import polars as pl

from sparql import fetch_property_statements, sparql_df


def _extract_qid(name: str = "item") -> pl.Expr:
    return pl.col(name).str.extract(r"^http://www.wikidata.org/entity/(Q\d+)$", 1)


def test_sparql_df():
    lf = sparql_df(
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


def test_fetch_property_statements() -> None:
    ldf = fetch_property_statements(pid="P9750")
    assert ldf.schema == {"subject": pl.Utf8, "object": pl.Utf8}
    df = ldf.collect()
    assert len(df) > 1
