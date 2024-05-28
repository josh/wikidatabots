from datetime import date

import polars as pl
from polars.testing import assert_frame_equal

from wd_appletv import find_wd_movie_via_search


def test_find_wd_movie_via_search() -> None:
    df = pl.LazyFrame(
        {
            "title": [
                "O Brother, Where Art Thou?",
                "O Brother, Where Are You?",
                "O Brother, Where Are Thou?",
            ],
            "published_at": [
                date(2000, 12, 22),
                date(2000, 10, 20),
                date(2000, 12, 22),
            ],
            "directors": [
                ["Joel Coen", "Ethan Coen"],
                ["Joel Coen"],
                ["Jim Coen", "Ethan Coen"],
            ],
        },
        schema={
            "title": pl.Utf8,
            "published_at": pl.Date,
            "directors": pl.List(pl.Utf8),
        },
    )
    df1 = (
        df.pipe(find_wd_movie_via_search)
        .select("results")
        .explode("results")
        .unnest("results")
        .select("item")
    )
    df2 = pl.LazyFrame(
        {
            "item": [
                "http://www.wikidata.org/entity/Q501874",
                "http://www.wikidata.org/entity/Q501874",
                "http://www.wikidata.org/entity/Q501874",
            ]
        }
    )
    assert_frame_equal(df1, df2)
