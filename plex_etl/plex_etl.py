# pyright: basic

import pandas as pd

from sparql import sparql_csv

from .utils import decode_plex_guids


def wd_plex_guids() -> pd.DataFrame:
    query = "SELECT DISTINCT ?guid WHERE { ?item ps:P11460 ?guid. }"
    data = sparql_csv(query)
    df = pd.read_csv(data, dtype={"guid": "string"})
    df2 = decode_plex_guids(df["guid"])
    df = pd.concat([df, df2], axis=1).dropna().sort_values("key").reset_index(drop=True)
    return df
