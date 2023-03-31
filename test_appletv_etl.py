# pyright: strict

from datetime import date, datetime

import polars as pl
from polars.testing import assert_frame_equal

from appletv_etl import (
    append_jsonld_changes,
    cleaned_sitemap,
    fetch_jsonld_columns,
    siteindex,
    sitemap,
)


def test_siteindex():
    ldf = siteindex(type="show")
    assert ldf.schema == {"loc": pl.Utf8}
    ldf.collect()


def test_sitemap():
    ldf = sitemap(type="show", limit=5)
    assert ldf.schema == {
        "loc": pl.Utf8,
        "lastmod": pl.Datetime,
        "changefreq": pl.Categorical,
        "priority": pl.Float32,
    }
    ldf.collect()


def test_cleaned_sitemap():
    ldf = cleaned_sitemap(type="show", limit=5)
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


def test_append_jsonld_changes():
    sitemap_df = pl.LazyFrame(
        {
            "loc": [
                "https://tv.apple.com/us/show/umc.cmc.25tn3v8ku4b39tr6ccgb8nl6m",
                "https://tv.apple.com/us/movie/umc.cmc.3eh9r5iz32ggdm4ccvw5igiir",
                "https://tv.apple.com/us/movie/umc.cmc.1111111111111111111111111",
            ],
            "country": ["us", "us", "us"],
            "priority": [0.8, 0.5, 0.1],
        }
    )
    jsonld_df = pl.LazyFrame(
        {
            "loc": ["https://tv.apple.com/us/movie/umc.cmc.3eh9r5iz32ggdm4ccvw5igiir"],
            "jsonld_success": [True],
            "title": ["CODA"],
            "published_at": pl.Series([date(2021, 8, 13)], dtype=pl.Date),
            "director": ["Siân Heder"],
            "retrieved_at": pl.Series(
                [datetime(2023, 1, 1)], dtype=pl.Datetime(time_unit="ns")
            ),
        }
    )

    df = append_jsonld_changes(sitemap_df, jsonld_df, limit=3).drop("retrieved_at")
    df2 = pl.LazyFrame(
        {
            "loc": [
                "https://tv.apple.com/us/movie/umc.cmc.1111111111111111111111111",
                "https://tv.apple.com/us/movie/umc.cmc.3eh9r5iz32ggdm4ccvw5igiir",
                "https://tv.apple.com/us/show/umc.cmc.25tn3v8ku4b39tr6ccgb8nl6m",
            ],
            "jsonld_success": [False, True, True],
            "title": [None, "CODA", "The Morning Show"],
            "published_at": pl.Series(
                [None, date(2021, 8, 13), date(2019, 11, 1)], dtype=pl.Date
            ),
            "director": [None, "Siân Heder", None],
        }
    )
    assert_frame_equal(df, df2)
