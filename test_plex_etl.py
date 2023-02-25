import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from plex_etl import (
    decode_plex_guids,
    encode_plex_guids,
    extract_guids,
    fetch_metadata_guids,
    plex_search_guids,
    wikidata_plex_guids,
)

PLEX_TOKEN = os.environ.get("PLEX_TOKEN")


def setup_module() -> None:
    pl.toggle_string_cache(True)


def teardown_module() -> None:
    pl.toggle_string_cache(False)


def test_wikidata_plex_guids() -> None:
    ldf = wikidata_plex_guids()
    assert ldf.schema == {"key": pl.Binary}
    assert len(ldf.collect()) > 0


@pytest.mark.skipif(PLEX_TOKEN is None, reason="Missing PLEX_TOKEN")
def test_plex_search_guids():
    df = plex_search_guids(query="Top Gun").collect()
    assert df.schema == {"type": pl.Categorical, "key": pl.Binary}
    assert len(df) > 0


def test_decode_plex_guids() -> None:
    df = pl.DataFrame(
        {
            "guid": [
                "plex://episode/5d9c11154eefaa001f6364e0",
                "plex://movie/5d7768686f4521001eaa5cac",
                "plex://season/5d9c09bd3c3f87001f361344",
                "plex://show/5d9c08544eefaa001f5daa50",
                "plex://movie/000000000000000000000000",
                "plex://invalid/111111111111111111111111",
            ],
        }
    ).lazy()
    df2 = pl.DataFrame(
        {
            "type": pl.Series(
                [
                    "episode",
                    "movie",
                    "season",
                    "show",
                    "movie",
                    None,
                ],
                dtype=pl.Categorical,
            ),
            "key": [
                b"]\x9c\x11\x15N\xef\xaa\x00\x1fcd\xe0",
                b"]whhoE!\x00\x1e\xaa\\\xac",
                b"]\x9c\t\xbd<?\x87\x00\x1f6\x13D",
                b"]\x9c\x08TN\xef\xaa\x00\x1f]\xaaP",
                b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                None,
            ],
        }
    ).lazy()
    assert_frame_equal(decode_plex_guids(df), df2)


def test_encode_plex_guids() -> None:
    df1 = pl.DataFrame(
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
    ).lazy()
    df2 = pl.DataFrame(
        {
            "guid": [
                "plex://episode/5d9c11154eefaa001f6364e0",
                "plex://movie/5d7768686f4521001eaa5cac",
                "plex://season/5d9c09bd3c3f87001f361344",
                "plex://show/5d9c08544eefaa001f5daa50",
            ]
        }
    ).lazy()
    assert_frame_equal(encode_plex_guids(df1), df2)


def test_extract_guids() -> None:
    df = pl.DataFrame(
        {
            "text": [
                """
                1. plex://episode/5d9c11154eefaa001f6364e0
                "plex://movie/5d7768686f4521001eaa5cac"
                link='plex://season/5d9c09bd3c3f87001f361344'
                <plex://show/5d9c08544eefaa001f5daa50>
                """,
                """
                plex://movie/5d7768686f4521001eaa5cac
                plex://invalid/111111111111111111111111
                """,
            ]
        }
    ).lazy()
    df2 = pl.DataFrame(
        {
            "type": pl.Series(
                [
                    "movie",
                    "show",
                    "season",
                    "episode",
                ],
                dtype=pl.Categorical,
            ),
            "key": [
                bytes.fromhex("5d7768686f4521001eaa5cac"),
                bytes.fromhex("5d9c08544eefaa001f5daa50"),
                bytes.fromhex("5d9c09bd3c3f87001f361344"),
                bytes.fromhex("5d9c11154eefaa001f6364e0"),
            ],
        }
    ).lazy()
    assert_frame_equal(extract_guids(df), df2)


@pytest.mark.skipif(PLEX_TOKEN is None, reason="Missing PLEX_TOKEN")
def test_fetch_metadata_guids() -> None:
    df = pl.DataFrame(
        {
            "key": [
                bytes.fromhex("5d776be17a53e9001e732ab9"),
                bytes.fromhex("5d776824103a2d001f5639b0"),
                bytes.fromhex("5d9c0874ffd9ef001e99607a"),
                bytes.fromhex("5d9c07ea705e7a001e6cc76a"),
                bytes.fromhex("000000000000000000000000"),
            ]
        }
    ).lazy()
    df2 = pl.DataFrame(
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
            "imdb_numeric_id": pl.Series(
                [1745960, 119116, 72500, 348896, None], dtype=pl.UInt32
            ),
            "tmdb_id": pl.Series([361743, 18, 2207, 49075, None], dtype=pl.UInt32),
            "tvdb_id": pl.Series([16721, 305, 75932, 108501, None], dtype=pl.UInt32),
        }
    ).lazy()
    assert_frame_equal(fetch_metadata_guids(df).drop(["retrieved_at"]), df2)
