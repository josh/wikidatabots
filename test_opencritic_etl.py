# pyright: strict

import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from opencritic_etl import fetch_opencritic_game, opencritic_ratelimits


@pytest.mark.skipif("RAPIDAPI_KEY" not in os.environ, reason="Missing RAPIDAPI_KEY")
def test_opencritic_ratelimits() -> None:
    df = opencritic_ratelimits().collect()
    assert df["searches_limit"].item() == 25
    assert df["requests_limit"].item() == 200


@pytest.mark.skipif("RAPIDAPI_KEY" not in os.environ, reason="Missing RAPIDAPI_KEY")
def test_fetch_game() -> None:
    df1 = (
        pl.LazyFrame({"id": [1548, 14343]})
        .select(
            pl.col("id").pipe(fetch_opencritic_game).alias("metadata"),
        )
        .unnest("metadata")
        .select("id", "name", "url")
    )
    df2 = pl.LazyFrame(
        {
            "id": [1548, 14343],
            "name": [
                "The Legend of Zelda: Breath of the Wild",
                "The Legend of Zelda: Tears of the Kingdom",
            ],
            "url": [
                "https://opencritic.com/game/1548/"
                "the-legend-of-zelda-breath-of-the-wild",
                "https://opencritic.com/game/14343/"
                "the-legend-of-zelda-tears-of-the-kingdom",
            ],
        },
        schema={"id": pl.UInt32, "name": pl.Utf8, "url": pl.Utf8},
    )
    assert_frame_equal(df1, df2)
