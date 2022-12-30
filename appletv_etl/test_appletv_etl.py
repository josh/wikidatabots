# pyright: basic

import pandas as pd

from . import cleaned_sitemap, fetch_jsonld_df, siteindex


def test_siteindex():
    # siteindex(type="episode")
    siteindex(type="movie")
    # siteindex(type="show")


def test_sitemap():
    # cleaned_sitemap(type="episode")
    # cleaned_sitemap(type="movie")
    cleaned_sitemap(type="show")


def test_fetch_info_df():
    urls = pd.Series(
        [
            "https://tv.apple.com/us/show/umc.cmc.25tn3v8ku4b39tr6ccgb8nl6m",
            "https://tv.apple.com/us/movie/umc.cmc.3eh9r5iz32ggdm4ccvw5igiir",
            "https://tv.apple.com/us/movie/umc.cmc.1111111111111111111111111",
        ]
    )
    df = fetch_jsonld_df(urls)
    records = df.to_dict(orient="records")

    assert records[0]["jsonld_success"] is True
    assert records[0]["title"] == "The Morning Show"

    assert records[1]["jsonld_success"] is True
    assert records[1]["title"] == "CODA"

    assert records[2]["jsonld_success"] is False
    assert records[2]["title"] is pd.NA
