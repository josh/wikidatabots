import polars as pl

from wikidata_etl import fetch_property_class_constraints


def test_fetch_property_class_constraints() -> None:
    ldf = fetch_property_class_constraints("P4947")
    assert ldf.schema == {
        "numeric_pid": pl.UInt32,
        "pid": pl.Categorical,
        "class_numeric_qid": pl.UInt32,
        "class_qid": pl.Categorical,
        "class_label": pl.Utf8,
    }
    df = ldf.collect()
    assert len(df) > 10