import os

import pandas as pd
from plexapi.myplex import MyPlexAccount

from sparql import sparql_csv


def plex_connect():
    account = MyPlexAccount(
        username=os.environ["PLEX_USERNAME"],
        password=os.environ["PLEX_PASSWORD"],
        token=os.environ["PLEX_TOKEN"],
    )
    resource = account.resource(os.environ["PLEX_SERVER"])
    return resource.connect()


def plex_library_guids() -> pd.DataFrame:
    plex = plex_connect()
    guids = pd.Series([item.guid for item in plex.library.all()])
    df = decode_plex_guids(guids)
    df["guid"] = pd.Series(guids, dtype="string")
    df = df.dropna().sort_values("key").reset_index(drop=True)
    return df[["guid", "type", "key"]]


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
