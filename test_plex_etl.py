import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from plex_etl import (
    fetch_metadata_guids,
    fetch_person_guids,
    plex_search_guids,
    plex_server,
    wikidata_plex_media_guids,
    wikidata_plex_person_guids,
    wikidata_search_guids,
)

PLEX_TOKEN = os.environ.get("PLEX_TOKEN")
PLEX_SERVER = os.environ.get("PLEX_SERVER")


def setup_module() -> None:
    pl.enable_string_cache()


def teardown_module() -> None:
    pl.disable_string_cache()


def test_wikidata_plex_media_guids() -> None:
    ldf = wikidata_plex_media_guids()
    assert ldf.collect_schema() == pl.Schema({"key": pl.Binary})
    assert len(ldf.collect()) > 0


def test_wikidata_plex_person_guids() -> None:
    ldf = wikidata_plex_person_guids()
    assert ldf.collect_schema() == pl.Schema({"key": pl.Binary})
    assert len(ldf.collect()) > 0


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
        fetch_metadata_guids(df).drop(["retrieved_at", "similar_guids"]), df2
    )


@pytest.mark.skipif(PLEX_TOKEN is None, reason="Missing PLEX_TOKEN")
def test_fetch_person_guids() -> None:
    df = pl.LazyFrame(
        {
            "key": [
                bytes.fromhex("5d77682654f42c001f8c2650"),
                bytes.fromhex("5d776827999c64001ec2c986"),
                bytes.fromhex("5d776825151a60001f24a403"),
                bytes.fromhex("000000000000000000000000"),
            ]
        }
    )
    df2 = pl.LazyFrame(
        {
            "key": [
                bytes.fromhex("5d77682654f42c001f8c2650"),
                bytes.fromhex("5d776827999c64001ec2c986"),
                bytes.fromhex("5d776825151a60001f24a403"),
                bytes.fromhex("000000000000000000000000"),
            ],
            "type": pl.Series(
                ["person", "person", "person", None], dtype=pl.Categorical
            ),
            "success": [True, True, True, False],
            "slug": pl.Series(
                ["tom-hanks", "daniel-craig", "scarlett-johansson", None], dtype=pl.Utf8
            ),
            "name": pl.Series(
                ["Tom Hanks", "Daniel Craig", "Scarlett Johansson", None], dtype=pl.Utf8
            ),
        }
    )
    assert_frame_equal(fetch_person_guids(df).drop(["retrieved_at"]), df2)


def test_plex_search_guids() -> None:
    df = (
        pl.LazyFrame({"query": ["the godfather"]})
        .pipe(plex_search_guids)
        .with_columns(
            pl.col("key").bin.encode("hex").alias("hexkey"),
        )
        .collect()
    )
    assert df.collect_schema() == pl.Schema(
        {
            "key": pl.Binary(),
            "type": pl.Categorical(ordering="physical"),
            "hexkey": pl.Utf8(),
        }
    )
    assert len(df) > 0

    df2 = df.filter(
        (pl.col("type") == "movie") & (pl.col("hexkey") == "5d7768248a7581001f12bc72")
    )
    assert len(df2) == 1


def test_wikidata_search_guids() -> None:
    ldf = wikidata_search_guids(limit=10)
    assert ldf.collect_schema() == pl.Schema(
        {
            "key": pl.Binary(),
            "type": pl.Categorical(ordering="physical"),
        }
    )
    df = ldf.collect()
    assert len(df) > 0


@pytest.mark.skipif(PLEX_TOKEN is None, reason="Missing PLEX_TOKEN")
@pytest.mark.skipif(PLEX_SERVER is None, reason="Missing PLEX_SERVER")
def test_plex_server() -> None:
    assert PLEX_SERVER, "Missing PLEX_SERVER"
    ldf = plex_server(name=PLEX_SERVER)
    df = ldf.collect()
    assert len(df) == 1
    assert df.columns == ["name", "publicAddress", "accessToken", "uri"]
