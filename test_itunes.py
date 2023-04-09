# pyright: strict

import polars as pl
from polars.testing import assert_frame_equal

from itunes import batch_lookup, id_expr_ok


def test_batch_lookup_one() -> None:
    results = batch_lookup([285494571])
    (id, result) = list(results)[0]
    assert id == 285494571
    assert result
    assert result["trackName"] == "The Godfather"


def test_batch_lookup_miss() -> None:
    results = batch_lookup([200000])
    (id, result) = list(results)[0]
    assert id == 200000
    assert not result


def test_id_expr_ok() -> None:
    df1 = pl.LazyFrame({"id": [285494571, 200000]})
    df2 = pl.LazyFrame({"id": [285494571, 200000], "ok": [True, False]})
    df3 = df1.with_columns(
        pl.col("id").pipe(id_expr_ok, country="us").alias("ok"),
    )
    assert_frame_equal(df3, df2)
