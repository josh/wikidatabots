# pyright: strict

import datetime
import logging
import os
import random
from typing import Literal

import polars as pl

from polars_requests import (
    Session,
    prepare_request,
    response_date,
    response_text,
    urllib3_requests,
)
from polars_utils import outlier_exprs, update_or_append, update_parquet, xml_extract
from sparql import sparql_df

GUID_TYPE = Literal["episode", "movie", "season", "show"]

_GUID_RE = r"plex://(?P<type>episode|movie|season|show)/(?P<key>[a-f0-9]{24})"

_PLEX_SESSION = Session(
    headers={"X-Plex-Token": os.environ.get("PLEX_TOKEN", "")},
    ok_statuses={200, 404},
    read_timeout=15.0,
    retry_count=2,
)

_PLEX_SERVER_SESSION = Session()

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


def _plex_library_guids() -> pl.LazyFrame:
    return (
        _plex_server(name=os.environ["PLEX_SERVER"])
        .with_columns(pl.lit([[1, 2]]).alias("section"))
        .explode("section")
        .select(
            pl.format("{}/library/sections/{}/all", pl.col("uri"), pl.col("section"))
            .pipe(prepare_request, headers={"X-Plex-Token": pl.col("accessToken")})
            .pipe(
                urllib3_requests,
                session=_PLEX_SERVER_SESSION,
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
        .unique(subset="key", maintain_order=True)
    )


def wikidata_plex_guids() -> pl.LazyFrame:
    return (
        sparql_df(
            "SELECT DISTINCT ?guid WHERE { ?item ps:P11460 ?guid. }",
            columns=["guid"],
        )
        .select(pl.col("guid").pipe(_decode_plex_guid).alias("key"))
        .drop_nulls()
        .unique(subset="key", maintain_order=True)
    )


_SEARCH_METACONTAINER_JSON_DTYPE: pl.PolarsDataType = pl.Struct(
    {
        "MediaContainer": pl.Struct(
            {
                "SearchResults": pl.List(
                    pl.Struct(
                        {
                            "SearchResult": pl.List(
                                pl.Struct({"Metadata": pl.Struct({"guid": pl.Utf8})})
                            ),
                        }
                    )
                )
            }
        )
    }
)


def plex_search_guids(expr: pl.Expr) -> pl.Expr:
    return (
        prepare_request(
            url=pl.lit("https://metadata.provider.plex.tv/library/search"),
            fields={
                "query": expr,
                "limit": "100",
                "searchTypes": "movie,tv",
                "includeMetadata": "1",
            },
            headers={
                "Accept": "application/json",
                "X-Plex-Token": os.environ["PLEX_TOKEN"],
            },
        )
        .pipe(urllib3_requests, session=_PLEX_SESSION, log_group="plex_metadata_search")
        .pipe(response_text)
        .str.json_extract(_SEARCH_METACONTAINER_JSON_DTYPE)
        .struct.field("MediaContainer")
        .struct.field("SearchResults")
        .arr.eval(
            pl.element()
            .struct.field("SearchResult")
            .arr.eval(pl.element().struct.field("Metadata").struct.field("guid"))
            .flatten()
        )
        .flatten()
        .unique()
        .pipe(_decode_plex_guid)
    )


_TITLE_QUERY = """
SELECT ?title WHERE {
  SERVICE bd:sample {
    ?item wdt:P4947 _:b1.
    bd:serviceParam bd:sample.limit ?limit ;
      bd:sample.sampleType "RANDOM".
  }
  ?item wdt:P1476 ?title.
  OPTIONAL { ?item wdt:P11460 ?plex_guid. }
  FILTER(!(BOUND(?plex_guid)))
}
"""


def _wd_random_titles(limit: int) -> pl.LazyFrame:
    return sparql_df(_TITLE_QUERY.replace("?limit", str(limit)), columns=["title"])


_SEARCH_LIMIT = 10


def wikidata_search_guids() -> pl.LazyFrame:
    return (
        _wd_random_titles(limit=_SEARCH_LIMIT)
        .rename({"title": "query"})
        .select(pl.col("query").pipe(plex_search_guids).alias("key"))
    )


def _decode_plex_guid(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(_GUID_RE, 2).str.decode("hex")


def _sort(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.sort(by=pl.col("key").bin.encode("hex"))


_OLDEST_METADATA = pl.col("retrieved_at").rank("ordinal") < 2_000
_MISSING_METADATA = pl.col("retrieved_at").is_null()


def _backfill_metadata(df: pl.LazyFrame, predicate: pl.Expr) -> pl.LazyFrame:
    df = df.cache()

    df_updated = (
        df.filter(_OLDEST_METADATA | _MISSING_METADATA | predicate)
        .pipe(fetch_metadata_guids)
        .cache()
    )

    df_similar = (
        df_updated.select("similar_keys")
        .explode("similar_keys")
        .rename({"similar_keys": "key"})
        .drop_nulls()
        .unique(subset="key", maintain_order=True)
        .cache()
    )

    return (
        df.pipe(update_or_append, df_updated.drop("similar_keys"), on="key")
        .pipe(update_or_append, df_similar, on="key")
        .pipe(_sort)
    )


_THIS_YEAR = datetime.date.today().year


def outlier_expr(df: pl.DataFrame) -> pl.Expr:
    exprs = df.pipe(
        outlier_exprs,
        [
            (pl.col("type") == "movie").alias("type_movie"),
            (pl.col("type") == "show").alias("type_show"),
            pl.col("success"),
            (pl.col("year") < _THIS_YEAR).alias("past_year"),
            pl.col("imdb_numeric_id"),
            pl.col("tmdb_id"),
            pl.col("tvdb_id"),
        ],
        rmax=3,
        max_count=1_000,
    )

    expr_str, expr, count = random.choice(exprs)
    logging.info(f"Refreshing {count:,} outlier rows against `{expr_str}`")

    return expr


_METADATA_DTYPE = pl.Struct(
    {
        "guid": pl.Utf8,
        "ratingKey": pl.Utf8,
        "type": pl.Categorical,
        "title": pl.Utf8,
        "year": pl.UInt16,
        "Similar": pl.List(pl.Struct({"guid": pl.Utf8})),
        "Guid": pl.List(pl.Struct({"id": pl.Utf8})),
    }
)

_METACONTAINER_JSON_DTYPE: pl.PolarsDataType = pl.Struct(
    {"MediaContainer": pl.Struct({"Metadata": pl.List(_METADATA_DTYPE)})}
)


def fetch_metadata_guids(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.with_columns(
            pl.format(
                "https://metadata.provider.plex.tv/library/metadata/{}",
                pl.col("key").bin.encode("hex"),
            )
            .pipe(prepare_request, headers={"Accept": "application/json"})
            .pipe(
                urllib3_requests,
                session=_PLEX_SESSION,
                log_group="metadata.provider.plex.tv/library/metadata",
            ),
        )
        .with_columns(
            pl.col("response").struct.field("status").alias("status_code"),
            (
                pl.col("response")
                .pipe(response_date)
                .cast(pl.Datetime(time_unit="ns"))
                .alias("retrieved_at")
            ),
            (
                pl.col("response")
                .pipe(response_text)
                .str.json_extract(_METACONTAINER_JSON_DTYPE)
                .struct.field("MediaContainer")
                .struct.field("Metadata")
                .arr.first()
                .alias("metadata")
            ),
        )
        .select(
            pl.col("key"),
            pl.col("metadata").struct.field("type").alias("type"),
            (pl.col("status_code") == 200).alias("success"),
            pl.col("retrieved_at"),
            pl.col("metadata").struct.field("year").alias("year"),
            _extract_guid(r"imdb://(?:tt|nm)(\d+)").alias("imdb_numeric_id"),
            _extract_guid(r"tmdb://(\d+)").alias("tmdb_id"),
            _extract_guid(r"tvdb://(\d+)").alias("tvdb_id"),
            (
                pl.col("metadata")
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
        pl.col("metadata")
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
    return (
        plex_df.pipe(update_or_append, _plex_library_guids(), on="key")
        .pipe(update_or_append, wikidata_plex_guids(), on="key")
        .pipe(_sort)
    )


def main() -> None:
    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        return df.pipe(_discover_guids).pipe(
            _backfill_metadata, predicate=outlier_expr(df.collect())
        )

    with pl.StringCache():
        update_parquet("plex.parquet", update, key="key")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
