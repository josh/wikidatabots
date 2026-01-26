import polars as pl

from sparql import sparql


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
    assert lf.collect_schema() == pl.Schema(
        {
            "item": pl.Utf8,
            "itemLabel": pl.Utf8,
            "qid": pl.Utf8,
        }
    )
    df = lf.collect()
    assert len(df) == 10
