# pyright: strict

from datetime import date

import polars as pl
from polars.testing import assert_frame_equal

from appletv_etl import cleaned_sitemap, fetch_jsonld_columns, siteindex, sitemap


def test_siteindex() -> None:
    ldf = siteindex(sitemap_type="show")
    assert ldf.schema == {"loc": pl.Utf8}
    ldf.collect()


def test_sitemap() -> None:
    ldf = sitemap(sitemap_type="show", limit=5)
    assert ldf.schema == {
        "loc": pl.Utf8,
        "lastmod": pl.Datetime,
        "priority": pl.Float32,
    }
    ldf.collect()


def test_cleaned_sitemap() -> None:
    ldf = cleaned_sitemap(sitemap_type="show", limit=5)
    assert ldf.schema == {
        "id": pl.Utf8,
        "type": pl.Categorical,
        "country": pl.Categorical,
        "loc": pl.Utf8,
        "lastmod": pl.Datetime(time_unit="ns"),
        "priority": pl.Float32,
        "in_latest_sitemap": pl.Boolean,
    }
    ldf.collect()


def test_fetch_jsonld_columns() -> None:
    df = (
        pl.LazyFrame(
            {
                "loc": [
                    "https://tv.apple.com/us/show/umc.cmc.25tn3v8ku4b39tr6ccgb8nl6m",
                    "https://tv.apple.com/us/movie/umc.cmc.3eh9r5iz32ggdm4ccvw5igiir",
                    "https://tv.apple.com/us/movie/umc.cmc.3vmofz00i7y5m00861o6waay8",
                    "https://tv.apple.com/us/movie/umc.cmc.1111111111111111111111111",
                ]
            }
        )
        .pipe(fetch_jsonld_columns)
        .drop("retrieved_at")
    )

    df2 = pl.LazyFrame(
        {
            "loc": [
                "https://tv.apple.com/us/show/umc.cmc.25tn3v8ku4b39tr6ccgb8nl6m",
                "https://tv.apple.com/us/movie/umc.cmc.3eh9r5iz32ggdm4ccvw5igiir",
                "https://tv.apple.com/us/movie/umc.cmc.3vmofz00i7y5m00861o6waay8",
                "https://tv.apple.com/us/movie/umc.cmc.1111111111111111111111111",
            ],
            "jsonld_success": [True, True, True, False],
            "title": ["The Morning Show", "CODA", "O Brother, Where Art Thou?", None],
            "published_at": pl.Series(
                [date(2019, 11, 1), date(2021, 8, 13), date(2001, 2, 2), None],
                dtype=pl.Date,
            ),
            "directors": [None, ["Siân Heder"], ["Joel Coen", "Ethan Coen"], None],
            "itunes_id": pl.Series([None, None, 188765152, None], dtype=pl.UInt64),
        },
    )

    assert_frame_equal(df, df2)
