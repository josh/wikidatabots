import os

import pandas as pd
from plexapi.myplex import MyPlexAccount

from .utils import decode_plex_guids


def plex_connect():
    account = MyPlexAccount(
        username=os.environ["PLEX_USERNAME"],
        password=os.environ["PLEX_PASSWORD"],
        token=os.environ["PLEX_TOKEN"],
    )
    resource = account.resource(os.environ["PLEX_SERVER"])
    return resource.connect()


def plex_library_guids():
    plex = plex_connect()
    guids = pd.Series([item.guid for item in plex.library.all()])
    df = decode_plex_guids(guids)
    df["guid"] = pd.Series(guids, dtype="string")
    df = df.dropna().sort_values("key").reset_index(drop=True)
    return df[["guid", "type", "key"]]
