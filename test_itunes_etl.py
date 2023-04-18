# pyright: strict

import polars as pl
from hypothesis import given, settings
from hypothesis import strategies as st
from polars.testing import assert_frame_equal

from itunes_etl import check_itunes_id, lookup_itunes_id, wikidata_itunes_all_ids

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


def test_check_itunes_id() -> None:
    df1 = pl.DataFrame(
        {"id": [1440768692, 909253, 1, 1440768764, 2, 909253]},
        schema={"id": pl.UInt64},
    ).with_columns(
        pl.col("id").pipe(check_itunes_id, country="us").alias("country_us"),
    )
    df2 = pl.DataFrame(
        {
            "id": [1440768692, 909253, 1, 1440768764, 2, 909253],
            "country_us": [True, True, False, True, False, True],
        },
        schema={"id": pl.UInt64, "country_us": pl.Boolean},
    )
    assert_frame_equal(df1, df2)


def test_wikidata_itunes_all_ids() -> None:
    ldf = wikidata_itunes_all_ids()
    assert ldf.schema == {"id": pl.UInt64}
    ldf.collect()
