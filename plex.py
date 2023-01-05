import pandas as pd
import requests

from sparql import sparql_csv


def plex_server(name: str, token: str) -> pd.Series:
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


def plex_library_guids(baseuri: str, token: str) -> pd.DataFrame:
    dfs = [plex_library_section_guids(baseuri, token, s) for s in [1, 2]]
    df = pd.concat(dfs, ignore_index=True)
    df2 = decode_plex_guids(df["guid"])
    df = pd.concat([df, df2], axis=1)
    df = df.dropna().sort_values("key").reset_index(drop=True)
    return df


def plex_library_section_guids(baseuri: str, token: str, section: int) -> pd.DataFrame:
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
