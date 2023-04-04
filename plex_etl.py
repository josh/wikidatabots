# pyright: strict

import logging
import os
from typing import Literal

import polars as pl

from polars_requests import (
    Session,
    prepare_request,
    response_date,
    response_text,
    urllib3_requests,
)
from polars_utils import update_or_append, update_parquet, xml_extract
from sparql import sparql_df

GUID_TYPE = Literal["episode", "movie", "person", "season", "show"]

_GUID_RE = r"plex://(?P<type>episode|movie|person|season|show)/(?P<key>[a-f0-9]{24})"

_PLEX_SESSION = Session(
    headers={"X-Plex-Token": os.environ.get("PLEX_TOKEN", "")},
    ok_statuses={200, 404},
)

_PLEX_DEVICE_DTYPE = pl.Struct(
    {
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
)


def _plex_server(name: str) -> pl.LazyFrame:
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
                xml_extract,
                dtype=pl.List(_PLEX_DEVICE_DTYPE),
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


def _plex_library_guids(server_df: pl.LazyFrame) -> pl.LazyFrame:
    # TODO: Avoid collect
    server = server_df.collect().row(index=0, named=True)
    plex_server_session = Session(headers={"X-Plex-Token": server["accessToken"]})

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
                xml_extract,
                dtype=pl.List(pl.Struct({"guid": pl.Utf8})),
                log_group="parse_xml",
            )
            .alias("item")
        )
        .explode("item")
        .select(
            pl.col("item").struct.field("guid").alias("guid"),
        )
        .select(pl.col("guid").pipe(_decode_plex_guid).alias("key"))
        .drop_nulls()
        .unique(subset="key")
    )


def wikidata_plex_guids() -> pl.LazyFrame:
    return (
        sparql_df(
            "SELECT DISTINCT ?guid WHERE { ?item ps:P11460 ?guid. }",
            columns=["guid"],
        )
        .select(pl.col("guid").pipe(_decode_plex_guid).alias("key"))
        .drop_nulls()
        .unique(subset="key")
    )


def _decode_plex_guid(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(_GUID_RE, 2).str.decode("hex").cast(pl.Binary)


_OLDEST_METADATA = pl.col("retrieved_at").rank("ordinal") < 1_000
_MISSING_METADATA = pl.col("retrieved_at").is_null()


def _backfill_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.cache()

    df_updated = (
        df.filter(_OLDEST_METADATA | _MISSING_METADATA)
        .pipe(fetch_metadata_guids)
        .cache()
    )

    df_similar = (
        df_updated.select("similar_keys")
        .explode("similar_keys")
        .rename({"similar_keys": "key"})
        .drop_nulls()
        .unique(subset="key")
        .cache()
    )

    return (
        df.pipe(update_or_append, df_updated.drop("similar_keys"), on="key")
        .pipe(update_or_append, df_similar, on="key")
        .sort(by=pl.col("key").bin.encode("hex"))
    )


def _fetch_metadata_text(df: pl.LazyFrame) -> pl.LazyFrame:
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


_METADATA_XML_SCHEMA: dict[str, pl.PolarsDataType] = {
    "type": pl.Categorical,
    "year": pl.Utf8,
    "Guid": pl.List(pl.Struct({"id": pl.Utf8})),
    "Similar": pl.List(pl.Struct({"guid": pl.Utf8})),
}


def fetch_metadata_guids(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.pipe(_fetch_metadata_text)
        .with_columns(
            (
                pl.col("response_text")
                .pipe(
                    xml_extract,
                    dtype=pl.List(pl.Struct(_METADATA_XML_SCHEMA)),
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
            pl.col("video").struct.field("year").cast(pl.UInt16).alias("year"),
            _extract_guid(r"imdb://(?:tt|nm)(\d+)").alias("imdb_numeric_id"),
            _extract_guid(r"tmdb://(\d+)").alias("tmdb_id"),
            _extract_guid(r"tvdb://(\d+)").alias("tvdb_id"),
            (
                pl.col("video")
                .struct.field("Similar")
                .arr.eval(
                    pl.element()
                    .struct.field("guid")
                    .str.extract(_GUID_RE, 2)
                    .str.decode("hex")
                )
                .alias("similar_keys")
            ),
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


def encode_plex_guids(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        pl.format(
            "plex://{}/{}",
            pl.col("type"),
            pl.col("key").bin.encode("hex"),
        ).alias("guid")
    )


def _discover_guids(plex_df: pl.LazyFrame) -> pl.LazyFrame:
    server_df = _plex_server(name=os.environ["PLEX_SERVER"])

    dfs = [
        _plex_library_guids(server_df),
        wikidata_plex_guids(),
    ]
    df_new = pl.concat(
        dfs,
        parallel=False,  # BUG: parallel caching is broken
    ).unique(subset="key")

    return plex_df.join(df_new, on="key", how="outer").sort(
        by=pl.col("key").bin.encode("hex")
    )


def main() -> None:
    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        return df.pipe(_discover_guids).pipe(_backfill_metadata)

    with pl.StringCache():
        update_parquet("plex.parquet", update)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
