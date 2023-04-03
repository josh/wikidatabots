# pyright: strict

import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from plex_etl import encode_plex_guids, fetch_metadata_guids, wikidata_plex_guids

PLEX_TOKEN = os.environ.get("PLEX_TOKEN")


def setup_module() -> None:
    pl.toggle_string_cache(True)


def teardown_module() -> None:
    pl.toggle_string_cache(False)


def test_wikidata_plex_guids() -> None:
    ldf = wikidata_plex_guids()
    assert ldf.schema == {"key": pl.Binary}
    assert len(ldf.collect()) > 0


def test_encode_plex_guids() -> None:
    df1 = pl.LazyFrame(
        {
            "type": pl.Series(
                ["episode", "movie", "season", "show"], dtype=pl.Categorical
            ),
            "key": [
                b"]\x9c\x11\x15N\xef\xaa\x00\x1fcd\xe0",
                b"]whhoE!\x00\x1e\xaa\\\xac",
                b"]\x9c\t\xbd<?\x87\x00\x1f6\x13D",
                b"]\x9c\x08TN\xef\xaa\x00\x1f]\xaaP",
            ],
        }
    )
    df2 = pl.LazyFrame(
        {
            "type": pl.Series(
                ["episode", "movie", "season", "show"], dtype=pl.Categorical
            ),
            "key": [
                b"]\x9c\x11\x15N\xef\xaa\x00\x1fcd\xe0",
                b"]whhoE!\x00\x1e\xaa\\\xac",
                b"]\x9c\t\xbd<?\x87\x00\x1f6\x13D",
                b"]\x9c\x08TN\xef\xaa\x00\x1f]\xaaP",
            ],
            "guid": [
                "plex://episode/5d9c11154eefaa001f6364e0",
                "plex://movie/5d7768686f4521001eaa5cac",
                "plex://season/5d9c09bd3c3f87001f361344",
                "plex://show/5d9c08544eefaa001f5daa50",
            ],
        }
    )
    assert_frame_equal(encode_plex_guids(df1), df2)


@pytest.mark.skipif(PLEX_TOKEN is None, reason="Missing PLEX_TOKEN")
def test_fetch_metadata_guids() -> None:
    df = pl.LazyFrame(
        {
            "key": [
                bytes.fromhex("5d776be17a53e9001e732ab9"),
                bytes.fromhex("5d776824103a2d001f5639b0"),
                bytes.fromhex("5d9c0874ffd9ef001e99607a"),
                bytes.fromhex("5d9c07ea705e7a001e6cc76a"),
                bytes.fromhex("000000000000000000000000"),
            ]
        }
    )
    df2 = pl.LazyFrame(
        {
            "key": [
                bytes.fromhex("5d776be17a53e9001e732ab9"),
                bytes.fromhex("5d776824103a2d001f5639b0"),
                bytes.fromhex("5d9c0874ffd9ef001e99607a"),
                bytes.fromhex("5d9c07ea705e7a001e6cc76a"),
                bytes.fromhex("000000000000000000000000"),
            ],
            "type": pl.Series(
                ["movie", "movie", "show", "show", None], dtype=pl.Categorical
            ),
            "success": [True, True, True, True, False],
            "year": pl.Series([2022, 1997, 1975, 2002, None], dtype=pl.UInt16),
            "imdb_numeric_id": pl.Series(
                [1745960, 119116, 72500, 348896, None], dtype=pl.UInt32
            ),
            "tmdb_id": pl.Series([361743, 18, 2207, 49075, None], dtype=pl.UInt32),
            "tvdb_id": pl.Series([16721, 305, 75932, 108501, None], dtype=pl.UInt32),
        }
    )
    assert_frame_equal(
        fetch_metadata_guids(df).drop(["retrieved_at", "similar_keys"]), df2
    )
