import numpy as np
import pandas as pd
import pyarrow as pa

from .utils import (
    decode_plex_guids,
    encode_plex_guids,
    pack_plex_keys,
    unpack_plex_keys,
)


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
