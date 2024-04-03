# pyright: strict


import polars as pl
from polars.testing import assert_frame_equal

from opencritic_etl import fetch_opencritic_game


def test_fetch_game() -> None:
    df1 = (
        pl.LazyFrame({"id": [1, 1548, 14343]}, schema={"id": pl.UInt32})
        .with_columns(
            pl.col("id").pipe(fetch_opencritic_game).alias("metadata"),
        )
        .unnest("metadata")
        .select("id", "name", "url")
    )
    df2 = pl.LazyFrame(
        {
            "id": [1, 1548, 14343],
            "name": [
                None,
                "The Legend of Zelda: Breath of the Wild",
                "The Legend of Zelda: Tears of the Kingdom",
            ],
            "url": [
                None,
                "https://opencritic.com/game/1548/"
                "the-legend-of-zelda-breath-of-the-wild",
                "https://opencritic.com/game/14343/"
                "the-legend-of-zelda-tears-of-the-kingdom",
            ],
        },
        schema={"id": pl.UInt32, "name": pl.Utf8, "url": pl.Utf8},
    )
    assert_frame_equal(df1, df2)
