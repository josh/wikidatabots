import pandas as pd

QUERY = """
SELECT ?plex_guid WHERE {
  ?item ps:P11460 ?plex_guid.
}
"""

GUID_RE = r"plex://(?P<type>episode|movie|season|show)/(?P<key>[a-f0-9]{24})"


def decode_plex_guids(guids: pd.Series) -> pd.DataFrame:
    df = guids.str.extract(GUID_RE).astype({"type": "category", "key": "string"})
    df["key"] = pack_plex_keys(df["key"])
    return df


def encode_plex_guids(df: pd.DataFrame) -> pd.Series:
    return "plex://" + df["type"] + "/" + df["key"].pipe(unpack_plex_keys)


def pack_plex_keys(keys: pd.Series) -> pd.Series:
    return keys.map(bytes.fromhex, na_action="ignore").astype("binary[pyarrow]")


def unpack_plex_keys(keys: pd.Series) -> pd.Series:
    return keys.apply(bytes.hex).astype("string")
