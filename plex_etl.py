# pyright: strict

import os

import polars as pl

from polars_requests import (
    Session,
    prepare_request,
    response_date,
    response_text,
    urllib3_requests,
)
from polars_utils import apply_with_tqdm, read_xml, update_ipc, update_or_append
from sparql import sparql_df

_GUID_RE = r"plex://(?P<type>movie|show|season|episode)/(?P<key>[a-f0-9]{24})"

_PLEX_SESSION = Session(
    headers={"X-Plex-Token": os.environ.get("PLEX_TOKEN", "")},
    ok_statuses={200, 404},
)

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


def _parse_device_xml(text: str) -> pl.Series:
    return read_xml(text, schema=_PLEX_DEVICE_SCHEMA).to_struct("Device")


def plex_server(name: str) -> pl.LazyFrame:
    return (
        pl.LazyFrame({"url": ["https://plex.tv/api/resources"]})
        .select(
            pl.col("url")
            .pipe(prepare_request)
            .pipe(
                urllib3_requests,
                session=_PLEX_SESSION,
                log_group="plex.tv/api/resources",
            )
            .pipe(response_text)
            .pipe(
                apply_with_tqdm,
                _parse_device_xml,
                return_dtype=pl.List(pl.Struct(_PLEX_DEVICE_SCHEMA)),
                log_group="parse_device_xml",
            )
            .alias("Device")
        )
        .explode("Device")
        .select(
            pl.col("Device").struct.field("name").alias("name"),
            pl.col("Device").struct.field("publicAddress").alias("publicAddress"),
            pl.col("Device").struct.field("accessToken").alias("accessToken"),
            pl.col("Device").struct.field("Connection").alias("Connection"),
        )
        .filter(pl.col("name") == name)
        .explode("Connection")
        .filter(pl.col("Connection").struct.field("local") == "0")
        .with_columns(
            pl.col("Connection").struct.field("uri"),
        )
        .drop("Connection")
    )


def plex_library_guids(server_df: pl.LazyFrame) -> pl.LazyFrame:
    # TODO: Avoid collect
    server = server_df.collect().row(index=0, named=True)
    plex_server_session = Session(headers={"X-Plex-Token": server["accessToken"]})

    def _parse_xml(text: str) -> pl.Series:
        return read_xml(text, schema={"guid": pl.Utf8}).to_struct("item")

    return (
        pl.LazyFrame({"section": [1, 2]})
        .select(
            pl.format(
                "{}/library/sections/{}/all", pl.lit(server["uri"]), pl.col("section")
            )
            .pipe(prepare_request)
            .pipe(
                urllib3_requests,
                session=plex_server_session,
                log_group="plexserver/library/sections/all",
            )
            .pipe(response_text)
            .pipe(
                apply_with_tqdm,
                _parse_xml,
                return_dtype=pl.List(pl.Struct({"guid": pl.Utf8})),
                log_group="parse_xml",
            )
            .alias("item")
        )
        .explode("item")
        .select(
            pl.col("item").struct.field("guid").alias("guid"),
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


def plex_similar(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.pipe(fetch_metadata_text)
        .select(pl.col("response_text").alias("text"))
        .pipe(extract_guids)
        .select(["key"])
        .drop_nulls()
        .unique(subset="key")
    )


def backfill_missing_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.cache()
    return df.pipe(
        update_or_append,
        df.filter(pl.col("retrieved_at").is_null()).pipe(fetch_metadata_guids),
        on="key",
    ).sort(by=pl.col("key").bin.encode("hex"))


_METADATA_XML_SCHEMA: dict[str, pl.PolarsDataType] = {
    "type": pl.Categorical,
    "Guid": pl.List(pl.Struct({"id": pl.Utf8})),
}


def fetch_metadata_text(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        pl.format(
            "https://metadata.provider.plex.tv/library/metadata/{}",
            pl.col("key").bin.encode("hex"),
        )
        .pipe(prepare_request)
        .pipe(
            urllib3_requests,
            session=_PLEX_SESSION,
            log_group="metadata.provider.plex.tv/library/metadata",
        ),
    ).with_columns(
        pl.col("response").struct.field("status").alias("status_code"),
        (
            pl.col("response")
            .pipe(response_date)
            .cast(pl.Datetime(time_unit="ns"))
            .alias("retrieved_at")
        ),
        pl.col("response").pipe(response_text).alias("response_text"),
    )


def fetch_metadata_guids(df: pl.LazyFrame) -> pl.LazyFrame:
    def parse_response_text(text: str) -> pl.Series:
        return read_xml(
            text,
            schema=_METADATA_XML_SCHEMA,
            xpath="./*",
        ).to_struct("video")

    return (
        df.pipe(fetch_metadata_text)
        .with_columns(
            (
                pl.col("response_text")
                .pipe(
                    apply_with_tqdm,
                    parse_response_text,
                    return_dtype=pl.List(pl.Struct(_METADATA_XML_SCHEMA)),
                    log_group="parse_response_text",
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
            _extract_guid(r"imdb://(?:tt|nm)(\d+)").alias("imdb_numeric_id"),
            _extract_guid(r"tmdb://(\d+)").alias("tmdb_id"),
            _extract_guid(r"tvdb://(\d+)").alias("tvdb_id"),
        )
    )


def _extract_guid(pattern: str) -> pl.Expr:
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


def _discover_guids(plex_df: pl.LazyFrame) -> pl.LazyFrame:
    server_df = plex_server(name=os.environ["PLEX_SERVER"])

    dfs = [
        plex_library_guids(server_df),
        wikidata_plex_guids(),
        (plex_df.select(["key"]).collect().sample(n=500).lazy().pipe(plex_similar)),
    ]
    df_new = pl.concat(
        dfs,
        parallel=False,  # BUG: parallel caching is broken
    ).unique(subset="key")

    return plex_df.join(df_new, on="key", how="outer").sort(
        by=pl.col("key").bin.encode("hex")
    )


def main_discover_guids() -> None:
    with pl.StringCache():
        update_ipc("plex.arrow", _discover_guids)


def main_metadata() -> None:
    with pl.StringCache():
        update_ipc("plex.arrow", backfill_missing_metadata)
