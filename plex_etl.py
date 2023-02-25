# pyright: strict

import os
from typing import Any

import polars as pl
import requests

from polars_requests import (
    response_series_date,
    response_series_status_code,
    response_series_text,
)
from polars_utils import read_xml, update_ipc
from sparql import sparql_df

_GUID_RE = r"plex://(?P<type>movie|show|season|episode)/(?P<key>[a-f0-9]{24})"
_SESSION = requests.Session()

_PLEX_DEVICE_SCHEMA: dict[str, pl.PolarsDataType] = {
    "name": pl.Utf8,
    "publicAddress": pl.Utf8,
    "accessToken": pl.Utf8,
    "Connection": pl.List(
        pl.Struct(
            {
                "uri": pl.Utf8,
                "local": pl.Utf8,
            }
        )
    ),
}


def plex_server(name: str) -> dict[str, Any]:
    url = "https://plex.tv/api/resources"
    headers = {"X-Plex-Token": os.environ["PLEX_TOKEN"]}
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    return (
        read_xml(r.text, schema=_PLEX_DEVICE_SCHEMA)
        .explode("Connection")
        .filter(pl.col("name") == name)
        .filter(pl.col("Connection").struct.field("local") == "0")
        .with_columns(
            pl.col("Connection").struct.field("uri"),
        )
        .drop("Connection")
        .row(index=0, named=True)
    )


def plex_library_guids(baseuri: str, server_token: str) -> pl.LazyFrame:
    session = requests.Session()
    session.headers.update({"X-Plex-Token": server_token})

    def _request(url: str) -> pl.Series:
        r = session.get(url)
        r.raise_for_status()
        return read_xml(r.text, schema={"guid": pl.Utf8}).to_struct("video")

    return (
        pl.DataFrame(
            {
                "url": [
                    f"{baseuri}/library/sections/1/all",
                    f"{baseuri}/library/sections/2/all",
                ]
            }
        )
        .lazy()
        .select(
            pl.col("url")
            .apply(
                _request,
                return_dtype=pl.List(pl.Struct({"guid": pl.Utf8})),
            )
            .alias("video")
        )
        .explode("video")
        .select(
            pl.col("video").struct.field("guid").alias("guid"),
        )
        .pipe(decode_plex_guids)
        .select(["key"])
        .drop_nulls()
        .unique(subset="key")
    )


def wikidata_plex_guids() -> pl.LazyFrame:
    return (
        sparql_df(
            "SELECT DISTINCT ?guid WHERE { ?item ps:P11460 ?guid. }",
            columns=["guid"],
        )
        .pipe(decode_plex_guids)
        .select(["key"])
        .drop_nulls()
        .unique(subset="key")
    )


def _plex_search(query: str) -> requests.Response:
    url = "https://metadata.provider.plex.tv/library/search"
    params = {
        "query": query,
        "limit": "100",
        "searchTypes": "movie,tv",
        "includeMetadata": "1",
    }
    headers = {"Accept": "application/json", "X-Plex-Token": os.environ["PLEX_TOKEN"]}
    r = _SESSION.get(url, headers=headers, params=params)
    return r


def plex_similar(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.pipe(fetch_metadata_text)
        .select(pl.col("response_text").alias("text"))
        .pipe(extract_guids)
        .select(["key"])
        .drop_nulls()
        .unique(subset="key")
    )


def plex_search_guids(query: str) -> pl.LazyFrame:
    return (
        pl.DataFrame({"query": [query]})
        .lazy()
        .select(
            pl.col("query")
            .apply(_plex_search, return_dtype=pl.Object)
            .map(response_series_text, return_dtype=pl.Utf8)
            .alias("text")
        )
        .pipe(extract_guids)
    )


def backfill_missing_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.cache()
    df2 = df.filter(pl.col("retrieved_at").is_null()).pipe(fetch_metadata_guids)
    return (
        pl.concat([df, df2])
        .unique(subset="key", keep="last")
        .sort(by=pl.col("key").bin.encode("hex"))
    )


_METADATA_XML_SCHEMA = {
    "type": pl.Categorical,
    "Guid": pl.List(pl.Struct({"id": pl.Utf8})),
}


def fetch_metadata_text(df: pl.LazyFrame) -> pl.LazyFrame:
    def _request_metadata(keys: pl.Series) -> pl.Series:
        return pl.Series([request_metadata(k) for k in keys])

    return df.with_columns(
        pl.col("key").map(_request_metadata).alias("response"),
    ).with_columns(
        pl.col("response").map(response_series_status_code).alias("status_code"),
        pl.col("response")
        .map(response_series_date)
        .cast(pl.Datetime(time_unit="ns"))
        .alias("retrieved_at"),
        pl.col("response")
        .map(response_series_text, return_dtype=pl.Utf8)
        .alias("response_text"),
    )


def fetch_metadata_guids(df: pl.LazyFrame) -> pl.LazyFrame:
    def parse_response_text(text: str) -> pl.Series:
        return read_xml(
            text,
            schema=_METADATA_XML_SCHEMA,
            xpath="./*",
        ).to_struct("video")

    def extract_guid(pattern: str) -> pl.Expr:
        return (
            pl.col("video")
            .struct.field("Guid")
            .arr.eval(
                pl.element()
                .struct.field("id")
                .str.extract(pattern, 1)
                .cast(pl.UInt32)
                .drop_nulls(),
            )
            .arr.first()
        )

    return (
        df.pipe(fetch_metadata_text)
        .with_columns(
            (
                pl.col("response_text")
                .apply(
                    parse_response_text,
                    return_dtype=pl.List(pl.Struct(_METADATA_XML_SCHEMA)),
                )
                .arr.first()
                .alias("video")
            ),
        )
        .select(
            pl.col("key"),
            pl.col("video").struct.field("type").alias("type"),
            (pl.col("status_code") == 200).alias("success"),
            pl.col("retrieved_at"),
            extract_guid(r"imdb://(?:tt|nm)(\d+)").alias("imdb_numeric_id"),
            extract_guid(r"tmdb://(\d+)").alias("tmdb_id"),
            extract_guid(r"tvdb://(\d+)").alias("tvdb_id"),
        )
    )


def request_metadata(key: bytes) -> requests.Response:
    assert len(key) == 12
    url = f"https://metadata.provider.plex.tv/library/metadata/{key.hex()}"
    headers = {"X-Plex-Token": os.environ["PLEX_TOKEN"]}
    r = _SESSION.get(url, headers=headers)
    return r


def extract_guids(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.select(
            pl.col("text").str.extract_all(_GUID_RE).alias("guid"),
        )
        .explode("guid")
        .pipe(decode_plex_guids)
        .drop_nulls()
        .unique()
        .sort(by="key")
    )


def decode_plex_guids(guids: pl.LazyFrame) -> pl.LazyFrame:
    return guids.select(
        pl.col("guid").str.extract(_GUID_RE, 1).cast(pl.Categorical).alias("type"),
        (
            pl.col("guid")
            .str.extract(_GUID_RE, 2)
            .str.decode("hex")
            .cast(pl.Binary)  # TODO: Binary dtype wrong on lazy frame
            .alias("key")
        ),
    )


def encode_plex_guids(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        pl.format(
            "plex://{}/{}",
            pl.col("type"),
            pl.col("key").bin.encode("hex"),
        ).alias("guid")
    )


def main_discover_guids() -> None:
    with pl.StringCache():
        server = plex_server(name=os.environ["PLEX_SERVER"])

        dfs = [
            plex_library_guids(server["uri"], server["accessToken"]),
            wikidata_plex_guids(),
            (
                pl.scan_ipc("s3://wikidatabots/plex.arrow")
                .select(["key"])
                .collect()
                .sample(n=500)
                .lazy()
                .pipe(plex_similar)
            ),
        ]
        df_new = pl.concat(dfs).unique(subset="key")

        df = (
            pl.scan_ipc("plex.arrow")
            .join(df_new, on="key", how="outer")
            .sort(by=pl.col("key").bin.encode("hex"))
        )
        df.collect().write_ipc("plex.arrow", compression="lz4")


def main_metadata() -> None:
    with pl.StringCache():
        update_ipc("plex.arrow", backfill_missing_metadata)
