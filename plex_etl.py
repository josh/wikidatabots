import os
import sys
from typing import Literal

import polars as pl

from polars_requests import prepare_request, request, response_date, response_text
from polars_utils import (
    SomeFrame,
    lazy_map_reduce_batches,
    update_or_append,
    update_parquet,
    xml_extract,
)
from sparql import sparql

GUID_TYPE = Literal["episode", "movie", "season", "show"]

_GUID_RE = r"plex://(?P<type>episode|movie|season|show)/(?P<key>[a-f0-9]{24})"
_ANY_KEY_RE = r"(plex://(episode|movie|season|show)/)?([a-f0-9]{24})"

_PLEX_API_RETRY_COUNT = 5

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


def plex_server(name: str) -> pl.LazyFrame:
    return (
        pl.LazyFrame({"url": ["https://plex.tv/api/resources"]})
        .select(
            pl.col("url")
            .pipe(prepare_request, headers={"X-Plex-Token": os.environ["PLEX_TOKEN"]})
            .pipe(request, log_group="plex.tv/api/resources")
            .pipe(response_text)
            .pipe(
                xml_extract,
                dtype=pl.List(_PLEX_DEVICE_DTYPE),
                log_group="parse_device_xml",
            )
            .alias("Device")
        )
        .explode("Device")
        .unnest("Device")
        .filter(pl.col("name") == name)
        .explode("Connection")
        .filter(pl.col("Connection").struct.field("local") == "0")
        .with_columns(
            pl.col("Connection").struct.field("uri"),
        )
        .drop("Connection")
    )


def _decode_plex_guid_key(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(_GUID_RE, 2).str.decode("hex")


def plex_library_guids() -> pl.LazyFrame:
    return (
        plex_server(name=os.environ["PLEX_SERVER"])
        .with_columns(pl.lit(pl.Series([[1, 2]])).alias("section"))
        .explode("section")
        .select(
            pl.format("{}/library/sections/{}/all", pl.col("uri"), pl.col("section"))
            .pipe(prepare_request, headers={"X-Plex-Token": pl.col("accessToken")})
            .pipe(
                request,
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
        .unnest("item")
        .select(
            pl.col("guid").pipe(_decode_plex_guid_key).alias("key"),
            pl.col("guid").pipe(_decode_plex_guid_type).alias("type"),
        )
        .drop_nulls()
        .unique(subset="key")
    )


def _decode_plex_any_key(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(_ANY_KEY_RE, 3).str.decode("hex")


def wikidata_plex_guids() -> pl.LazyFrame:
    return (
        sparql(
            "SELECT DISTINCT ?guid WHERE { ?item ps:P11460 ?guid. }",
            columns=["guid"],
        )
        .select(
            pl.col("guid").pipe(_decode_plex_any_key).alias("key"),
        )
        .drop_nulls()
        .unique(subset="key")
    )


_SEARCH_METACONTAINER_JSON_DTYPE = pl.Struct(
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


def plex_search_guids(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.select(
            prepare_request(
                url=pl.lit("https://discover.provider.plex.tv/library/search"),
                fields={
                    "query": pl.col("query"),
                    "limit": "100",
                    "searchTypes": "movies,tv",
                    "includeMetadata": "1",
                    "searchProviders": "discover",
                },
                headers={
                    "Accept": "application/json",
                    "X-Plex-Provider-Version": "6.3.0",
                    # "X-Plex-Token": os.environ["PLEX_TOKEN"],
                },
            )
            .pipe(
                request,
                log_group="plex_metadata_search",
                retry_count=_PLEX_API_RETRY_COUNT,
            )
            .pipe(response_text)
            .str.json_decode(_SEARCH_METACONTAINER_JSON_DTYPE)
            .struct.field("MediaContainer")
            .struct.field("SearchResults")
            .list.eval(
                pl.element()
                .struct.field("SearchResult")
                .list.eval(pl.element().struct.field("Metadata").struct.field("guid"))
                .flatten()
            )
            .flatten()
            .alias("guid")
        )
        .unique("guid")
        .select(
            pl.col("guid").pipe(_decode_plex_guid_key).alias("key"),
            pl.col("guid").pipe(_decode_plex_guid_type).alias("type"),
        )
        .drop_nulls()
        .unique(subset="key")
    )


_MOVIE_TITLE_QUERY = """
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

_TV_TITLE_QUERY = """
SELECT ?title WHERE {
  SERVICE bd:sample {
    ?item wdt:P4983 _:b1.
    bd:serviceParam bd:sample.limit ?limit ;
      bd:sample.sampleType "RANDOM".
  }
  ?item wdt:P1476 ?title.
  OPTIONAL { ?item wdt:P11460 ?plex_guid. }
  FILTER(!(BOUND(?plex_guid)))
}
"""

_SEARCH_LIMIT = 50


def _wd_random_titles(limit: int, tmdb_type: Literal["movie", "tv"]) -> pl.LazyFrame:
    if tmdb_type == "movie":
        query = _MOVIE_TITLE_QUERY
    elif tmdb_type == "tv":
        query = _TV_TITLE_QUERY
    return sparql(query.replace("?limit", str(limit)), columns=["title"])


def wikidata_search_guids(limit: int = _SEARCH_LIMIT) -> pl.LazyFrame:
    return (
        pl.concat(
            [
                _wd_random_titles(limit=limit, tmdb_type="movie"),
                _wd_random_titles(limit=limit, tmdb_type="tv"),
            ]
        )
        .select(
            pl.col("title").str.replace(r"#|&|'|\"", "").alias("query"),
        )
        .pipe(plex_search_guids)
    )


def _decode_plex_guid_type(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(_GUID_RE, 1).cast(pl.Categorical)


def _sort(df: SomeFrame) -> SomeFrame:
    return df.sort(by=pl.col("key").bin.encode("hex"))


_METADATA_DTYPE = pl.Struct(
    {
        "guid": pl.Utf8,
        "ratingKey": pl.Utf8,
        "type": pl.Utf8,
        "title": pl.Utf8,
        "year": pl.UInt16,
        "Similar": pl.List(pl.Struct({"guid": pl.Utf8})),
        "Guid": pl.List(pl.Struct({"id": pl.Utf8})),
    }
)

_METACONTAINER_JSON_DTYPE = pl.Struct(
    {"MediaContainer": pl.Struct({"Metadata": pl.List(_METADATA_DTYPE)})}
)


def _extract_guid(pattern: str) -> pl.Expr:
    return (
        pl.col("metadata")
        .struct.field("Guid")
        .list.eval(
            pl.element()
            .struct.field("id")
            .str.extract(pattern, 1)
            .cast(pl.UInt32)
            .drop_nulls(),
        )
        .list.first()
    )


def fetch_metadata_guids(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.with_columns(
            pl.format(
                "https://metadata.provider.plex.tv/library/metadata/{}",
                pl.col("key").bin.encode("hex"),
            )
            .pipe(
                prepare_request,
                headers={
                    "Accept": "application/json",
                    "X-Plex-Token": os.environ["PLEX_TOKEN"],
                },
            )
            .pipe(
                request,
                log_group="metadata.provider.plex.tv/library/metadata",
                ok_statuses={200, 404},
                bad_statuses={502, 504, 520},
                retry_count=_PLEX_API_RETRY_COUNT,
                timeout=60.0,
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
                .str.json_decode(_METACONTAINER_JSON_DTYPE)
                .struct.field("MediaContainer")
                .struct.field("Metadata")
                .list.first()
                .alias("metadata")
            ),
        )
        .select(
            pl.col("key"),
            pl.col("metadata").struct.field("type").cast(pl.Categorical).alias("type"),
            (pl.col("status_code") == 200).alias("success"),
            pl.col("retrieved_at"),
            pl.col("metadata").struct.field("year").alias("year"),
            _extract_guid(r"imdb://(?:tt|nm)(\d+)").alias("imdb_numeric_id"),
            _extract_guid(r"tmdb://(\d+)").alias("tmdb_id"),
            _extract_guid(r"tvdb://(\d+)").alias("tvdb_id"),
            (
                pl.col("metadata")
                .struct.field("Similar")
                .list.eval(pl.element().struct.field("guid"))
                .alias("similar_guids")
            ),
        )
    )


_OLDEST_METADATA = pl.col("retrieved_at").rank("ordinal") <= 1_500
_MISSING_METADATA = pl.col("retrieved_at").is_null()


def _backfill_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
    def map_metadata(df: pl.LazyFrame) -> pl.LazyFrame:
        return df.filter(_OLDEST_METADATA | _MISSING_METADATA).pipe(
            fetch_metadata_guids
        )

    def reduce_metadata(df: pl.DataFrame, df_metadata: pl.DataFrame) -> pl.DataFrame:
        df_updated, df_similar_guids = (
            df_metadata.drop("similar_guids"),
            df_metadata.select("similar_guids"),
        )

        df_similar = (
            df_similar_guids.explode("similar_guids")
            .rename({"similar_guids": "guid"})
            .select(
                pl.col("guid").pipe(_decode_plex_guid_key).alias("key"),
                pl.col("guid").pipe(_decode_plex_guid_type).alias("type"),
            )
            .drop_nulls()
            .unique(subset="key")
        )

        return (
            df.pipe(update_or_append, df_updated, on="key")
            .pipe(update_or_append, df_similar, on="key")
            .pipe(_sort)
        )

    return df.pipe(
        lazy_map_reduce_batches,
        map_function=map_metadata,
        reduce_function=reduce_metadata,
    )


def _discover_guids(plex_df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        plex_df.pipe(update_or_append, plex_library_guids(), on="key")
        .pipe(update_or_append, wikidata_plex_guids(), on="key")
        .pipe(update_or_append, wikidata_search_guids(), on="key")
        .pipe(_sort)
    )


def _log_retrieved_at(df: pl.DataFrame) -> pl.DataFrame:
    retrieved_at = df.select(pl.col("retrieved_at").min()).item()
    print(f"Oldest retrieved_at: {retrieved_at}", file=sys.stderr)
    return df


def _main() -> None:
    pl.enable_string_cache()

    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.pipe(_discover_guids)
            .pipe(_backfill_metadata)
            .map_batches(_log_retrieved_at)
        )

    update_parquet("plex.parquet", update, key="key")


if __name__ == "__main__":
    _main()
