# pyright: strict

import html
import urllib.parse
import zlib
from typing import Literal

import polars as pl
import requests
from bs4 import BeautifulSoup

from polars_utils import (
    read_xml,
    request_text,
    series_apply_with_tqdm,
    timestamp,
    update_ipc,
)

Type = Literal["episode", "movie", "show"]

SITEINDEX_SCHEMA: dict[str, pl.PolarsDataType] = {
    "loc": pl.Utf8,
}
SITEINDEX_DTYPE: pl.PolarsDataType = pl.List(pl.Struct(SITEINDEX_SCHEMA))


def siteindex(type: Type) -> pl.LazyFrame:
    return (
        pl.DataFrame({"type": [type]})
        .lazy()
        .select(
            pl.format("https://tv.apple.com/sitemaps_tv_index_{}_1.xml", pl.col("type"))
            .map(request_text, return_dtype=pl.Utf8)
            .apply(_parse_siteindex_xml, return_dtype=SITEINDEX_DTYPE)
            .alias("siteindex"),
        )
        .explode("siteindex")
        .select(
            pl.col("siteindex").struct.field("loc").alias("loc"),
        )
    )


def _parse_siteindex_xml(text: str) -> pl.Series:
    return read_xml(text, schema=SITEINDEX_SCHEMA).to_struct("siteindex")


SITEMAP_SCHEMA: dict[str, pl.PolarsDataType] = {
    "loc": pl.Utf8,
    "lastmod": pl.Utf8,
    "changefreq": pl.Categorical,
    "priority": pl.Float32,
}
SITEMAP_DTYPE: pl.PolarsDataType = pl.List(pl.Struct(SITEMAP_SCHEMA))


def sitemap(type: Type) -> pl.LazyFrame:
    return (
        siteindex(type)
        .select(
            pl.col("loc")
            .map(_request_compressed_text, return_dtype=pl.Utf8)
            .apply(_parse_sitemap_xml, return_dtype=SITEMAP_DTYPE)
            .alias("sitemap")
        )
        .explode("sitemap")
        .select(
            pl.col("sitemap").struct.field("loc").alias("loc"),
            (
                pl.col("sitemap")
                .struct.field("lastmod")
                .str.strptime(datatype=pl.Datetime(time_unit="ns"), fmt="%+")
                .cast(pl.Datetime(time_unit="ns"))
                .alias("lastmod")
            ),
            pl.col("sitemap").struct.field("changefreq").alias("changefreq"),
            pl.col("sitemap").struct.field("priority").alias("priority"),
        )
    )


def _parse_sitemap_xml(text: str) -> pl.Series:
    return read_xml(text, schema=SITEMAP_SCHEMA).to_struct("sitemap")


# TODO: Can't return binary from map for some reason
def _request_compressed_text(urls: pl.Series) -> pl.Series:
    session = requests.Session()

    def get_text(url: str) -> str:
        r = session.get(url, timeout=5)
        return zlib.decompress(r.content, 16 + zlib.MAX_WBITS).decode("utf-8")

    return series_apply_with_tqdm(
        urls, get_text, return_dtype=pl.Utf8, desc="Fetching URLs"
    )


LOC_PATTERN = (
    r"https://tv.apple.com/"
    r"(?P<country>[a-z]{2})/"
    r"(?P<type>episode|movie|show)/"
    r"(?P<slug>[^/]*)/"
    r"(?P<id>umc.cmc.[0-9a-z]+)"
)


def cleaned_sitemap(type: Type) -> pl.LazyFrame:
    # TODO: str.extract should return a struct
    return (
        sitemap(type)
        .with_columns(
            (
                pl.col("loc")
                .str.extract(LOC_PATTERN, 1)
                .cast(pl.Categorical)
                .alias("country")
            ),
            (
                pl.col("loc")
                .str.extract(LOC_PATTERN, 2)
                .cast(pl.Categorical)
                .alias("type")
            ),
            (
                pl.col("loc")
                .str.extract(LOC_PATTERN, 3)
                .apply(urllib.parse.unquote, return_dtype=pl.Utf8)
                .alias("slug")
            ),
            pl.col("loc").str.extract(LOC_PATTERN, 4).alias("id"),
            LATEST_EXPR,
        )
        .select(
            [
                "loc",
                "country",
                "slug",
                "id",
                "priority",
                "in_latest_sitemap",
                "lastmod",
                "changefreq",
                "type",
            ]
        )
    )


JSONLD_DTYPE = pl.Struct(
    [
        pl.Field("name", pl.Utf8),
        pl.Field("datePublished", pl.Utf8),
        pl.Field("director", pl.List(pl.Struct([pl.Field("name", pl.Utf8)]))),
    ]
)

JSONLD_SUCCESS_EXPR = pl.col("jsonld").struct.field("name").is_not_null()
JSONLD_TITLE_EXPR = (
    pl.col("jsonld").struct.field("name").apply(html.unescape, return_dtype=pl.Utf8)
)
JSONLD_PUBLISHED_AT_EXPR = (
    pl.col("jsonld")
    .struct.field("datePublished")
    .str.strptime(datatype=pl.Date, fmt="%+")
)
JSONLD_DIRECTOR_EXPR = (
    pl.col("jsonld")
    .struct.field("director")
    .arr.first()
    .struct.field("name")
    .apply(html.unescape, return_dtype=pl.Utf8)
)


def fetch_jsonld_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.with_columns(
            pl.col("loc")
            .map(request_text, return_dtype=pl.Utf8)
            .map(_series_extract_jsonld, return_dtype=pl.Utf8)
            .str.json_extract(dtype=JSONLD_DTYPE)
            .alias("jsonld")
        )
        .with_columns(
            JSONLD_SUCCESS_EXPR.alias("jsonld_success"),
            JSONLD_TITLE_EXPR.alias("title"),
            JSONLD_PUBLISHED_AT_EXPR.alias("published_at"),
            JSONLD_DIRECTOR_EXPR.alias("director"),
            timestamp().alias("retrieved_at"),
        )
        .drop("jsonld")
    )


def _series_extract_jsonld(urls: pl.Series) -> pl.Series:
    return series_apply_with_tqdm(
        urls,
        _extract_jsonld,
        return_dtype=pl.Utf8,
        desc="Extracting JSON-LD",
    )


def _extract_jsonld(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    if soup.find("h1", string="This content is no longer available."):
        return None

    link = soup.find("link", attrs={"rel": "canonical"})
    if not link:
        return None

    scripts = soup.find_all("script", {"type": "application/ld+json"})
    for script in scripts:
        return script.text

    return None


def append_jsonld_changes(
    sitemap_df: pl.LazyFrame,
    jsonld_df: pl.LazyFrame,
    limit: int,
) -> pl.LazyFrame:
    sitemap_df, jsonld_df = sitemap_df.cache(), jsonld_df.cache()

    jsonld_new_df = (
        sitemap_df.join(jsonld_df, on="loc", how="left")
        .filter(pl.col("jsonld_success").is_null())
        .with_columns(
            pl.when(pl.col("country").eq("us"))
            .then(pl.col("priority") + 1)
            .otherwise(pl.col("priority"))
            .alias("priority")
        )
        .sort(by="priority", reverse=True)
        .head(limit)
        .select(["loc"])
        .pipe(fetch_jsonld_columns)
    )

    return (
        pl.concat([jsonld_df, jsonld_new_df])
        .unique(subset="loc", keep="last")
        .sort(by="loc")
    )


OUTDATED_EXPR = pl.lit(False).alias("in_latest_sitemap")
LATEST_EXPR = pl.lit(True).alias("in_latest_sitemap")


def main_sitemap(type: Type) -> None:
    def update_sitemap(df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            pl.concat(
                [
                    df.with_columns(OUTDATED_EXPR),
                    cleaned_sitemap(type).with_columns(LATEST_EXPR),
                ]
            )
            .unique(subset="loc", keep="last")
            .sort(by="loc")
        )

    with pl.StringCache():
        update_ipc("sitemap.arrow", update_sitemap)


def main_jsonld() -> None:
    with pl.StringCache():
        sitemap_df = pl.read_ipc("sitemap.arrow", memory_map=False).lazy()
        jsonld_df = pl.read_ipc("jsonld.arrow", memory_map=False).lazy()
        df = append_jsonld_changes(sitemap_df, jsonld_df, limit=1000)
        df.collect().write_ipc("jsonld.arrow", compression="lz4")
