# pyright: strict

from datetime import date

import polars as pl
from polars.testing import assert_frame_equal, assert_series_equal

from appletv_etl import (
    appletv_to_itunes_series,
    cleaned_sitemap,
    fetch_jsonld_columns,
    siteindex,
    sitemap,
)


def test_siteindex() -> None:
    ldf = siteindex(sitemap_type="show")
    assert ldf.schema == {"loc": pl.Utf8}
    ldf.collect()


def test_sitemap() -> None:
    ldf = sitemap(sitemap_type="show", limit=5)
    assert ldf.schema == {
        "loc": pl.Utf8,
        "lastmod": pl.Datetime,
        "changefreq": pl.Categorical,
        "priority": pl.Float32,
    }
    ldf.collect()


def test_cleaned_sitemap() -> None:
    ldf = cleaned_sitemap(sitemap_type="show", limit=5)
    assert ldf.schema == {
        "id": pl.Utf8,
        "type": pl.Categorical,
        "country": pl.Categorical,
        "slug": pl.Utf8,
        "loc": pl.Utf8,
        "lastmod": pl.Datetime(time_unit="ns"),
        "changefreq": pl.Categorical,
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
                    "https://tv.apple.com/us/movie/umc.cmc.1111111111111111111111111",
                ]
            }
        )
        .pipe(fetch_jsonld_columns)
        .drop(columns=["retrieved_at"])
    )

    df2 = pl.LazyFrame(
        {
            "loc": [
                "https://tv.apple.com/us/show/umc.cmc.25tn3v8ku4b39tr6ccgb8nl6m",
                "https://tv.apple.com/us/movie/umc.cmc.3eh9r5iz32ggdm4ccvw5igiir",
                "https://tv.apple.com/us/movie/umc.cmc.1111111111111111111111111",
            ],
            "jsonld_success": [True, True, False],
            "title": ["The Morning Show", "CODA", None],
            "published_at": pl.Series(
                [date(2019, 11, 1), date(2021, 8, 13), None], dtype=pl.Date
            ),
            "director": [None, "Siân Heder", None],
        },
    )

    assert_frame_equal(df, df2)


def test_appletv_to_itunes_series() -> None:
    s1 = pl.Series(["umc.cmc.459n4f98t82t8ommdoa7ebnny"])
    s2 = pl.Series([282875479], dtype=pl.UInt64)
    assert_series_equal(appletv_to_itunes_series(s1), s2)
