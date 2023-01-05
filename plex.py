import logging
import os
import re
from typing import Iterable, TypedDict

import pandas as pd
import requests
from tqdm import tqdm

from sparql import sparql_csv

PLEX_TOKEN = os.environ.get("PLEX_TOKEN")
PLEX_SERVER_NAME = os.environ.get("PLEX_SERVER")
PLEX_SERVER_TOKEN = os.environ.get("PLEX_SERVER_TOKEN")


class GUIDs(TypedDict):
    imdb_numeric_id: int | None
    tmdb_id: int | None
    tvdb_id: int | None


GUID_RE = r"plex://(?P<type>episode|movie|season|show)/(?P<key>[a-f0-9]{24})"

EXTERNAL_GUID_RE = (
    r"imdb://(?:tt|nm)(?P<imdb_numeric_id>[0-9]+)|"
    r"tmdb://(?P<tmdb_id>[0-9]+)|"
    r"tvdb://(?P<tvdb_id>[0-9]+)"
)

EXTERNAL_GUID_DTYPES = {
    "imdb_numeric_id": "UInt32",
    "tmdb_id": "UInt32",
    "tvdb_id": "UInt32",
}


def plex_server(name: str, token: str | None = PLEX_TOKEN) -> pd.Series:
    assert token, "Missing Plex token"
    url = "https://plex.tv/api/resources"
    headers = {"X-Plex-Token": token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    device_xpath = f"/MediaContainer/Device[@name='{name}']"
    devices_df = pd.read_xml(r.text, xpath=device_xpath, attrs_only=True)
    conns_df = pd.read_xml(r.text, xpath=f"{device_xpath}/Connection[@local=0]")

    device = devices_df.iloc[0]
    conn = conns_df.iloc[0]
    return pd.concat([device, conn])


def plex_library_guids(
    baseuri: str,
    token: str | None = PLEX_SERVER_TOKEN,
) -> pd.DataFrame:
    assert token, "Missing Plex server token"
    dfs = [plex_library_section_guids(baseuri, s, token) for s in [1, 2]]
    df = pd.concat(dfs, ignore_index=True)
    df2 = decode_plex_guids(df["guid"])
    df = pd.concat([df, df2], axis=1)
    df = df.dropna().sort_values("key").reset_index(drop=True)
    return df


def plex_library_section_guids(
    baseuri: str,
    section: int = 1,
    token: str | None = PLEX_SERVER_TOKEN,
) -> pd.DataFrame:
    url = f"{baseuri}/library/sections/{section}/all"
    headers = {"X-Plex-Token": token}
    df = pd.read_xml(url, storage_options=headers)
    return df[["guid"]].astype("string")


def wikidata_plex_guids() -> pd.DataFrame:
    query = "SELECT DISTINCT ?guid WHERE { ?item ps:P11460 ?guid. }"
    data = sparql_csv(query)
    df = pd.read_csv(data, dtype={"guid": "string"})
    df2 = decode_plex_guids(df["guid"])
    df = pd.concat([df, df2], axis=1)
    df = df.dropna().sort_values("key").reset_index(drop=True)
    return df


def fetch_metadata_guids(key: bytes, token: str | None = PLEX_TOKEN) -> GUIDs:
    assert len(key) == 12
    assert token, "Missing Plex token"

    result: GUIDs = {
        "imdb_numeric_id": None,
        "tmdb_id": None,
        "tvdb_id": None,
    }

    url = f"https://metadata.provider.plex.tv/library/metadata/{key.hex()}"
    headers = {"X-Plex-Token": token}
    xpath = "/MediaContainer/Video/Guid"

    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        pass
    elif r.status_code == 404:
        return result
    else:
        r.raise_for_status()
        return result

    try:
        df = pd.read_xml(r.text, xpath=xpath)
    except ValueError:
        return result

    for guid in df["id"]:
        m = re.match(EXTERNAL_GUID_RE, guid)
        if not m:
            logging.warning(f"Unhandled GUID: {guid}")
        elif m.group("imdb_numeric_id"):
            result["imdb_numeric_id"] = int(m.group("imdb_numeric_id"))
        elif m.group("tmdb_id"):
            result["tmdb_id"] = int(m.group("tmdb_id"))
        elif m.group("tvdb_id"):
            result["tvdb_id"] = int(m.group("tvdb_id"))
        else:
            assert False, "unreachable"

    return result


def fetch_plex_guids_df(
    keys: Iterable[bytes],
    token: str | None = PLEX_TOKEN,
    progress: bool = False,
) -> pd.DataFrame:
    def records():
        desc = "Fetch Plex metdata"
        for key in tqdm(keys, desc=desc, disable=not progress, unit="key"):
            yield fetch_metadata_guids(key, token=token)

    return pd.DataFrame.from_records(records()).astype(EXTERNAL_GUID_DTYPES)


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
