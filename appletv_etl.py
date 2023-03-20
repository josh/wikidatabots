# pyright: strict

import html
import urllib.parse
import zlib
from typing import Literal

import polars as pl
from bs4 import BeautifulSoup

from polars_requests import Session, prepare_request, response_text, urllib3_requests
from polars_utils import apply_with_tqdm, read_xml, timestamp, update_ipc

_APPLETV_SESSION = Session(
    connect_timeout=0.5,
    read_timeout=10.0,
    retry_statuses={502},
    retry_count=3,
    retry_backoff_factor=1.0,
)

Type = Literal["episode", "movie", "show"]

SITEINDEX_SCHEMA: dict[str, pl.PolarsDataType] = {
    "loc": pl.Utf8,
}
SITEINDEX_DTYPE: pl.PolarsDataType = pl.List(pl.Struct(SITEINDEX_SCHEMA))


def siteindex(type: Type) -> pl.LazyFrame:
    return (
        pl.LazyFrame({"type": [type]})
        .select(
            pl.format("https://tv.apple.com/sitemaps_tv_index_{}_1.xml", pl.col("type"))
            .pipe(prepare_request)
            .pipe(
                urllib3_requests,
                session=_APPLETV_SESSION,
                log_group="tv.apple.com/sitemaps_tv_index_type_1.xml",
            )
            .pipe(response_text)
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
    "changefreq": pl.Utf8,
    "priority": pl.Utf8,
}
SITEMAP_DTYPE: pl.PolarsDataType = pl.List(pl.Struct(SITEMAP_SCHEMA))


def sitemap(type: Type) -> pl.LazyFrame:
    return (
        siteindex(type)
        .select(
            pl.col("loc")
            .pipe(prepare_request)
            .pipe(
                urllib3_requests,
                session=_APPLETV_SESSION,
                log_group="tv.apple.com/sitemaps_tv_type.xml.gz",
            )
            .struct.field("data")
            .pipe(_zlib_decompress_expr)
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
            (
                pl.col("sitemap")
                .struct.field("changefreq")
                .cast(pl.Categorical)  # BUG: Workaround string to category panic
                .alias("changefreq")
            ),
            (
                pl.col("sitemap")
                .struct.field("priority")
                .cast(pl.Float32)
                .alias("priority")
            ),
        )
    )


def _zlib_decompress(data: bytes) -> str:
    return zlib.decompress(data, 16 + zlib.MAX_WBITS).decode("utf-8")


def _zlib_decompress_expr(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        _zlib_decompress,
        return_dtype=pl.Utf8,
        log_group="zlib_decompress",
        desc="Decompressing",
    )


def _parse_sitemap_xml(text: str) -> pl.Series:
    return read_xml(text, schema=SITEMAP_SCHEMA).to_struct("sitemap")


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
            .pipe(prepare_request)
            .pipe(urllib3_requests, session=_APPLETV_SESSION, log_group="tv.apple.com")
            .pipe(response_text)
            .pipe(_extract_jsonld_expr)
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


def _extract_jsonld_expr(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        _extract_jsonld,
        return_dtype=pl.Utf8,
        log_group="extract_jsonld",
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
        .sort(by="priority", descending=True)
        .head(limit)
        .select(["loc"])
        .pipe(fetch_jsonld_columns)
    )

    return (
        pl.concat(
            [jsonld_df, jsonld_new_df],
            parallel=False,  # BUG: parallel caching is broken
        )
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
                ],
                parallel=False,  # BUG: parallel caching is broken
            )
            .unique(subset="loc", keep="last")
            .sort(by="loc")
        )

    with pl.StringCache():
        update_ipc("sitemap.arrow", update_sitemap)


def main_jsonld() -> None:
    with pl.StringCache():
        sitemap_df = pl.scan_ipc("sitemap.arrow", memory_map=False)
        jsonld_df = pl.scan_ipc("jsonld.arrow", memory_map=False)
        df = append_jsonld_changes(sitemap_df, jsonld_df, limit=1000)
        df.collect().write_ipc("jsonld.arrow", compression="lz4")
