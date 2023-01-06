import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from plex import (
    PLEX_TOKEN,
    decode_plex_guids,
    encode_plex_guids,
    fetch_metadata_guids,
    fetch_plex_guids_df,
    pack_plex_keys,
    unpack_plex_keys,
    wikidata_plex_guids,
)


def test_wikidata_plex_guids():
    df = wikidata_plex_guids()
    assert len(df) > 0
    assert df.dtypes["guid"] == "string"
    assert df.dtypes["type"] == "category"
    assert df.dtypes["key"] == "binary[pyarrow]"
    assert df["guid"].is_unique
    assert df["key"].is_unique


def test_decode_plex_guids():
    guids = pd.Series(
        [
            "plex://episode/5d9c11154eefaa001f6364e0",
            "plex://movie/5d7768686f4521001eaa5cac",
            "plex://season/5d9c09bd3c3f87001f361344",
            "plex://show/5d9c08544eefaa001f5daa50",
            "plex://movie/000000000000000000000000",
            "plex://invalid/111111111111111111111111",
        ]
    )
    df = decode_plex_guids(guids)

    assert df.dtypes["type"] == "category"
    assert df["type"].tolist() == [
        "episode",
        "movie",
        "season",
        "show",
        "movie",
        np.nan,
    ]

    assert df.dtypes["key"] == "binary[pyarrow]"
    assert isinstance(df["key"][0], bytes)
    assert df["key"].tolist() == [
        b"]\x9c\x11\x15N\xef\xaa\x00\x1fcd\xe0",
        b"]whhoE!\x00\x1e\xaa\\\xac",
        b"]\x9c\t\xbd<?\x87\x00\x1f6\x13D",
        b"]\x9c\x08TN\xef\xaa\x00\x1f]\xaaP",
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        pd.NA,
    ]


def test_encode_plex_guids():
    types = pd.Series(["episode", "movie", "season", "show"])
    keys = pd.Series(
        [
            b"]\x9c\x11\x15N\xef\xaa\x00\x1fcd\xe0",
            b"]whhoE!\x00\x1e\xaa\\\xac",
            b"]\x9c\t\xbd<?\x87\x00\x1f6\x13D",
            b"]\x9c\x08TN\xef\xaa\x00\x1f]\xaaP",
        ]
    )
    df = pd.DataFrame({"type": types, "key": keys})
    guids = encode_plex_guids(df)

    assert guids.dtype == "string"
    assert guids.tolist() == [
        "plex://episode/5d9c11154eefaa001f6364e0",
        "plex://movie/5d7768686f4521001eaa5cac",
        "plex://season/5d9c09bd3c3f87001f361344",
        "plex://show/5d9c08544eefaa001f5daa50",
    ]


def test_pack_plex_keys():
    hex_keys = pd.Series(["1" * 24, "2" * 24, "a" * 24])
    bin_keys = pack_plex_keys(hex_keys)
    assert isinstance(bin_keys[0], bytes)
    assert bin_keys.dtype == "binary[pyarrow]"
    assert bin_keys.tolist() == [b"\x11" * 12, b"\x22" * 12, b"\xaa" * 12]


def test_unpack_plex_keys():
    bin_keys = pd.Series([b"\x11" * 12, b"\x22" * 12, b"\xaa" * 12])
    hex_keys = unpack_plex_keys(bin_keys)
    assert isinstance(hex_keys[0], str)
    assert hex_keys.dtype == "string"
    assert hex_keys.tolist() == ["1" * 24, "2" * 24, "a" * 24]


def test_arrow_compat():
    guids = pd.Series(
        [
            "plex://episode/5d9c11154eefaa001f6364e0",
            "plex://movie/5d7768686f4521001eaa5cac",
            "plex://season/5d9c09bd3c3f87001f361344",
            "plex://show/5d9c08544eefaa001f5daa50",
            "plex://invalid/111111111111111111111111",
        ]
    )
    df = decode_plex_guids(guids)
    table = pa.Table.from_pandas(df)
    df2 = table.to_pandas()
    assert df.dtypes["type"] == df2.dtypes["type"]
    assert df.dtypes["key"] == df2.dtypes["key"]
    assert df["type"].tolist() == df2["type"].tolist()
    assert df["key"].tolist() == df2["key"].tolist()


@pytest.mark.skipif(PLEX_TOKEN is None, reason="Missing PLEX_TOKEN")
def test_fetch_metadata_guids():
    key = bytes.fromhex("5d776be17a53e9001e732ab9")
    guids = fetch_metadata_guids(key, token=PLEX_TOKEN)
    assert guids["success"] is True
    assert guids["imdb_numeric_id"] == 1745960
    assert guids["tmdb_id"] == 361743
    assert guids["tvdb_id"] == 16721

    key = bytes.fromhex("5d776824103a2d001f5639b0")
    guids = fetch_metadata_guids(key, token=PLEX_TOKEN)
    assert guids["success"] is True
    assert guids["imdb_numeric_id"] == 119116
    assert guids["tmdb_id"] == 18
    assert guids["tvdb_id"] == 305

    key = bytes.fromhex("5d9c0874ffd9ef001e99607a")
    guids = fetch_metadata_guids(key, token=PLEX_TOKEN)
    assert guids["success"] is True
    assert guids["imdb_numeric_id"] is None
    assert guids["tmdb_id"] is None
    assert guids["tvdb_id"] is None


@pytest.mark.skipif(PLEX_TOKEN is None, reason="Missing PLEX_TOKEN")
def test_fetch_plex_guids_df():
    keys = pd.Series(
        [
            bytes.fromhex("5d776be17a53e9001e732ab9"),
            bytes.fromhex("5d776824103a2d001f5639b0"),
            bytes.fromhex("5d9c0874ffd9ef001e99607a"),
            bytes.fromhex("5d776825961905001eb90a22"),
        ],
        dtype="binary[pyarrow]",
    )
    df = fetch_plex_guids_df(keys, token=PLEX_TOKEN)

    assert df.dtypes["key"] == "binary[pyarrow]"
    assert df.dtypes["retrieved_at"] == "datetime64[ns]"
    assert df.dtypes["imdb_numeric_id"] == "UInt32"
    assert df.dtypes["tmdb_id"] == "UInt32"
    assert df.dtypes["tvdb_id"] == "UInt32"

    assert df.iloc[0]["success"]
    assert df.iloc[1]["success"]
    assert df.iloc[2]["success"]
    assert not df.iloc[3]["success"]

    assert df.iloc[0]["tmdb_id"] == 361743
    assert df.iloc[1]["tmdb_id"] == 18
    assert df.iloc[2]["tmdb_id"] is pd.NA
    assert df.iloc[3]["tmdb_id"] is pd.NA