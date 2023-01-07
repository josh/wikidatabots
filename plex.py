import logging
import os
import re
import xml.etree.ElementTree as ET
from typing import Iterable, Iterator, TypedDict

import pandas as pd
import requests
from tqdm import tqdm

from pandas_utils import df_upsert, safe_column_join, safe_row_concat
from sparql import sparql_csv

PLEX_TOKEN = os.environ.get("PLEX_TOKEN")
PLEX_SERVER_NAME = os.environ.get("PLEX_SERVER")
PLEX_SERVER_TOKEN = os.environ.get("PLEX_SERVER_TOKEN")


class GUIDs(TypedDict):
    success: bool
    imdb_numeric_id: int | None
    tmdb_id: int | None
    tvdb_id: int | None


GUID_RE = r"plex://(?P<type>movie|show|season|episode)/(?P<key>[a-f0-9]{24})"

GUID_TYPE_DYPE = pd.CategoricalDtype(categories=["movie", "show", "season", "episode"])

EXTERNAL_GUID_RE = (
    r"imdb://(?:tt|nm)(?P<imdb_numeric_id>[0-9]+)|"
    r"tmdb://(?P<tmdb_id>[0-9]+)|"
    r"tvdb://(?P<tvdb_id>[0-9]+)"
)

GUID_COMPONENT_DTYPES = {
    "guid": "string",
    "type": GUID_TYPE_DYPE,
    "key": "binary[pyarrow]",
}

EXTERNAL_GUID_DTYPES = {
    "success": "boolean",
    "retrieved_at": "datetime64[ns]",
    "imdb_numeric_id": "UInt32",
    "tmdb_id": "UInt32",
    "tvdb_id": "UInt32",
}

session = requests.Session()


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
    return pd.concat([device, conn], verify_integrity=True)


def plex_library_guids(
    baseuri: str,
    token: str | None = PLEX_SERVER_TOKEN,
) -> pd.DataFrame:
    assert token, "Missing Plex server token"
    dfs = [plex_library_section_guids(baseuri, s, token) for s in [1, 2]]
    df = safe_row_concat(dfs)
    df2 = decode_plex_guids(df["guid"])
    df = safe_column_join([df, df2])
    # TODO: Review this sort
    df = df.dropna().sort_values("key", ignore_index=True)

    # TODO: Clean up post conditions after things are working
    assert df.dtypes["guid"] == "string"
    assert df.dtypes["type"] == GUID_TYPE_DYPE
    assert df.dtypes["key"] == "binary[pyarrow]"
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
    # TODO: Review this concat/sort
    df = safe_column_join([df, df2])
    df = df.sort_values("key", ignore_index=True)

    # TODO: Clean up post conditions after things are working
    assert df.dtypes["guid"] == "string"
    assert df.dtypes["type"] == GUID_TYPE_DYPE
    assert df.dtypes["key"] == "binary[pyarrow]"
    return df


def plex_search(query: str, token: str | None = PLEX_TOKEN):
    assert token, "Missing Plex token"
    url = "https://metadata.provider.plex.tv/library/search"
    params = {
        "query": query,
        "limit": "100",
        "searchTypes": "movie,tv",
        "includeMetadata": "1",
    }
    headers = {"Accept": "application/json", "X-Plex-Token": token}
    r = session.get(url, headers=headers, params=params)
    return r


def plex_similar(
    keys: pd.Series,
    token: str | None = PLEX_TOKEN,
    progress: bool = False,
) -> pd.DataFrame:
    assert keys.dtype == "object" or keys.dtype == "binary[pyarrow]"

    def map_key(key: bytes):
        r = request_metdata(key, token)
        return extract_guids(r.text)

    tqdm.pandas(desc="Fetch Plex metdata", disable=not progress)
    dfs = keys.progress_apply(map_key)

    df = safe_row_concat(dfs).astype(GUID_COMPONENT_DTYPES)

    # TODO: Clean up post conditions after things are working
    assert df.dtypes["guid"] == "string"
    assert df.dtypes["type"] == GUID_TYPE_DYPE
    assert df.dtypes["key"] == "binary[pyarrow]"
    return df


def plex_search_guids(query: str, token: str | None = PLEX_TOKEN) -> pd.DataFrame:
    r = plex_search(query, token)
    return extract_guids(r.text)


def backfill_missing_metadata(df: pd.DataFrame, limit: int = 1000) -> pd.DataFrame:
    df_missing_metadata = (
        df[df["retrieved_at"].isna()][["guid", "type", "key"]]
        .head(limit)
        .reset_index(drop=True)
    )
    # TODO: Review this concat/sort
    metadata_df = fetch_plex_guids_df(df_missing_metadata["key"], progress=True)
    df_changes = safe_column_join([df_missing_metadata, metadata_df])
    df = df_upsert(df, df_changes, on="key").sort_values("key", ignore_index=True)

    # TODO: Clean up post conditions after things are working
    assert df.dtypes["guid"] == "string"
    assert df.dtypes["type"] == GUID_TYPE_DYPE
    assert df.dtypes["key"] == "binary[pyarrow]"
    return df


def fetch_plex_guids_df(
    keys: pd.Series,
    token: str | None = PLEX_TOKEN,
    progress: bool = False,
) -> pd.DataFrame:
    assert keys.dtype == "object" or keys.dtype == "binary[pyarrow]"
    tqdm.pandas(desc="Fetch Plex metdata", disable=not progress)
    records: pd.Series = keys.progress_apply(fetch_metadata_guids, token=token)
    columns = ["success", "imdb_numeric_id", "tmdb_id", "tvdb_id"]
    df = pd.DataFrame.from_records(list(records), columns=columns, index=keys.index)
    df["retrieved_at"] = pd.Timestamp.now().floor("s")
    return df.astype(EXTERNAL_GUID_DTYPES)


def fetch_metadata_guids(key: bytes, token: str | None = PLEX_TOKEN) -> GUIDs:
    assert len(key) == 12
    assert token, "Missing Plex token"

    result: GUIDs = {
        "success": False,
        "imdb_numeric_id": None,
        "tmdb_id": None,
        "tvdb_id": None,
    }

    r = request_metdata(key=key, token=token)
    if r.status_code == 200:
        pass
    elif r.status_code == 404:
        return result
    else:
        r.raise_for_status()
        return result

    result["success"] = True

    root = ET.fromstring(r.content)
    for guid in root.findall("./Video/Guid"):
        guid = guid.attrib["id"]
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


def request_metdata(key: bytes, token: str | None = PLEX_TOKEN) -> requests.Response:
    assert len(key) == 12
    assert token, "Missing Plex token"

    url = f"https://metadata.provider.plex.tv/library/metadata/{key.hex()}"
    headers = {"X-Plex-Token": token}
    r = session.get(url, headers=headers)
    return r


def extract_guids(text: str | Iterable[str]):
    guids = re_finditer(GUID_RE, text)
    df1 = pd.DataFrame(guids, columns=["guid"], dtype="string").drop_duplicates()
    df2 = decode_plex_guids(df1["guid"])
    df3 = safe_column_join([df1, df2]).sort_values("key", ignore_index=True)

    # TODO: Clean up post conditions after things are working
    assert df3.dtypes["guid"] == "string"
    assert df3.dtypes["type"] == GUID_TYPE_DYPE
    assert df3.dtypes["key"] == "binary[pyarrow]"
    return df3


def re_finditer(pattern: str, string: str | Iterable[str]) -> Iterator[str]:
    if isinstance(string, str):
        for m in re.finditer(pattern, string):
            yield m[0]
    elif isinstance(string, Iterable):
        for s in string:
            for m in re.finditer(pattern, s):
                yield m[0]
    else:
        raise TypeError()


def decode_plex_guids(guids: pd.Series) -> pd.DataFrame:
    assert guids.dtype == "string"
    df = guids.str.extract(GUID_RE).astype({"type": GUID_TYPE_DYPE, "key": "string"})
    return df.assign(key=pack_plex_keys(df["key"]))


def encode_plex_guids(df: pd.DataFrame) -> pd.Series:
    assert df["type"].dtype == GUID_TYPE_DYPE or df["type"].dtype == "string"
    assert df["key"].dtype == "object" or df["key"].dtype == "binary[pyarrow]"
    return "plex://" + df["type"].astype("string") + "/" + unpack_plex_keys(df["key"])


def pack_plex_keys(keys: pd.Series) -> pd.Series:
    assert keys.dtype == "string"
    return keys.map(bytes.fromhex, na_action="ignore").astype("binary[pyarrow]")


def unpack_plex_keys(keys: pd.Series) -> pd.Series:
    assert keys.dtype == "object" or keys.dtype == "binary[pyarrow]"
    return keys.apply(bytes.hex).astype("string")
