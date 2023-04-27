# pyright: strict

import polars as pl
from hypothesis import given, settings
from hypothesis import strategies as st
from polars.testing import assert_frame_equal

from itunes_etl import (
    appletv_redirect_url,
    fetch_metadata,
    itunes_legacy_view_url,
    lookup_itunes_id,
    wikidata_itunes_all_ids,
)


def setup_module() -> None:
    pl.enable_string_cache(True)


def teardown_module() -> None:
    pl.enable_string_cache(False)


_RESULT_DTYPE = pl.Struct(
    [
        pl.Field("id", pl.UInt64),
        pl.Field("type", pl.Utf8),
        pl.Field("name", pl.Utf8),
        pl.Field("kind", pl.Utf8),
    ]
)


@given(
    batch_size=st.integers(min_value=1, max_value=11),
)
@settings(deadline=None)
def test_lookup_itunes_id(batch_size: int) -> None:
    lf1 = pl.LazyFrame(
        {
            "id": [
                909253,
                1,
                1440768692,
                1440768764,
                909253,
                102225079,
                1438674900,
                284910350,
                1676858107,
                None,
                6446905902,
            ]
        },
        schema={"id": pl.UInt64},
    ).with_columns(
        pl.col("id")
        .pipe(lookup_itunes_id, country="us", batch_size=batch_size)
        .alias("result"),
    )
    lf2 = pl.LazyFrame(
        {
            "id": [
                909253,
                1,
                1440768692,
                1440768764,
                909253,
                102225079,
                1438674900,
                284910350,
                1676858107,
                None,
                6446905902,
            ],
            "result": [
                {
                    "id": 909253,
                    "type": "Artist",
                    "name": "Jack Johnson",
                },
                None,
                {
                    "id": 1440768692,
                    "type": "Album",
                    "name": "In Between Dreams",
                },
                {
                    "id": 1440768764,
                    "kind": "song",
                    "name": "Banana Pancakes",
                },
                {
                    "id": 909253,
                    "type": "Artist",
                    "name": "Jack Johnson",
                },
                {
                    "id": 102225079,
                    "type": "TV Show",
                    "name": "The Office",
                },
                {
                    "id": 1438674900,
                    "type": "TV Season",
                    "name": "The Office: The Complete Series",
                },
                {
                    "id": 284910350,
                    "kind": "software",
                    "name": "Yelp: Food, Delivery & Reviews",
                },
                {
                    "id": 1676858107,
                    "kind": "feature-movie",
                    "name": "Avatar: The Way of Water",
                },
                None,
                {
                    "id": 6446905902,
                    "kind": "ebook",
                    "name": "Make Something Wonderful",
                },
            ],
        },
        schema={"id": pl.UInt64, "result": _RESULT_DTYPE},
    )
    assert lf1.schema["result"] == _RESULT_DTYPE
    assert_frame_equal(lf1, lf2)


def test_lookup_itunes_id_empty() -> None:
    df1 = pl.DataFrame({"id": []}, schema={"id": pl.UInt64}).with_columns(
        pl.col("id").pipe(lookup_itunes_id, country="us").alias("result")
    )
    df2 = pl.DataFrame({"id": [], "result": []}, schema=df1.schema)
    assert_frame_equal(df1, df2)


def test_fetch_metadata() -> None:
    lf1 = (
        pl.LazyFrame(
            {
                "id": [
                    909253,
                    1,
                    1440768692,
                    1440768764,
                    909253,
                    102225079,
                    1438674900,
                    284910350,
                    1676858107,
                    6446905902,
                ]
            },
            schema={"id": pl.UInt64},
        )
        .pipe(fetch_metadata)
        .select("id", "type", "kind", "us_country", "ca_country", "any_country")
    )
    lf2 = pl.LazyFrame(
        {
            "id": [
                909253,
                1,
                1440768692,
                1440768764,
                909253,
                102225079,
                1438674900,
                284910350,
                1676858107,
                6446905902,
            ],
            "type": [
                "Artist",
                None,
                "Album",
                None,
                "Artist",
                "TV Show",
                "TV Season",
                None,
                None,
                None,
            ],
            "kind": [
                None,
                None,
                None,
                "song",
                None,
                None,
                None,
                "software",
                "feature-movie",
                "ebook",
            ],
            "us_country": [True, False, True, True, True, True, True, True, True, True],
            "ca_country": [
                True,
                False,
                True,
                True,
                True,
                True,
                False,
                True,
                True,
                True,
            ],
            "any_country": [
                True,
                False,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
                True,
            ],
        },
        schema={
            "id": pl.UInt64,
            "type": pl.Categorical,
            "kind": pl.Categorical,
            "us_country": pl.Boolean,
            "ca_country": pl.Boolean,
            "any_country": pl.Boolean,
        },
    )
    assert_frame_equal(lf1, lf2)


def test_itunes_legacy_view_url() -> None:
    df = pl.LazyFrame(
        {
            "id": [
                269572838,
                310633997,
                1202441786,
                1310877657,
                1265845136,
                1273427890,
                395536306,
            ],
            "type": ["Album", None, None, "Movie Bundle", None, "TV Season", None],
            "kind": [None, "software", "feature-movie", None, "podcast", None, "ebook"],
        },
        schema={"id": pl.UInt64, "type": pl.Categorical, "kind": pl.Categorical},
    )
    df2 = df.with_columns(
        itunes_legacy_view_url(
            id=pl.col("id"), type=pl.col("type"), kind=pl.col("kind")
        ).alias("view_url")
    )
    df3 = df.with_columns(
        pl.lit(
            [
                "https://itunes.apple.com/us/album/id269572838",
                "https://itunes.apple.com/us/app/id310633997",
                "https://itunes.apple.com/us/movie/id1202441786",
                "https://itunes.apple.com/us/movie-collection/id1310877657",
                "https://itunes.apple.com/us/podcast/id1265845136",
                "https://itunes.apple.com/us/tv-season/id1273427890",
                "https://itunes.apple.com/us/book/id395536306",
            ]
        ).alias("view_url")
    )
    assert_frame_equal(df2, df3)


def test_appletv_redirect_url() -> None:
    df = pl.LazyFrame(
        {
            "id": [1202441786, 1676858107, 1310877657, 310633997],
            "type": [None, None, "Movie Bundle", None],
            "kind": ["feature-movie", "feature-movie", None, "software"],
        },
        schema={
            "id": pl.UInt64,
            "type": pl.Categorical,
            "kind": pl.Categorical,
        },
    )
    df2 = df.with_columns(
        appletv_redirect_url(
            id=pl.col("id"),
            type=pl.col("type"),
            kind=pl.col("kind"),
        ).alias("appletv_redirect_url")
    )
    df3 = df.with_columns(
        pl.lit(
            [
                "https://tv.apple.com/us/movie/get-out/umc.cmc.2nh80sbq32nedy9rm09gtv8rb",
                "https://tv.apple.com/us/movie/avatar-the-way-of-water/umc.cmc.5k5xo2espahvd6kcswi2b5oe9",
                None,
                None,
            ]
        ).alias("appletv_redirect_url")
    )
    assert_frame_equal(df2, df3)


def test_wikidata_itunes_all_ids() -> None:
    ldf = wikidata_itunes_all_ids()
    assert ldf.schema == {"id": pl.UInt64}
    ldf.collect()
