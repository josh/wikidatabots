# pyright: strict

import html
import json
import logging
import re
import sys
import urllib.parse
import zlib
from typing import Any, Literal

import polars as pl
from bs4 import BeautifulSoup

from polars_requests import (
    Session,
    prepare_request,
    request,
    response_date,
    response_text,
)
from polars_utils import (
    apply_with_tqdm,
    head,
    limit,
    update_or_append,
    update_parquet,
    xml_extract,
)

_APPLETV_SESSION = Session(timeout=30.0, retry_count=10)

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/14.1.1 Safari/605.1.15"
)

_BROWSER_HEADERS: dict[str, str | pl.Expr] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-us",
    "User-Agent": _USER_AGENT,
}

_TYPE = Literal["movie", "episode", "show"]
_TYPES: set[_TYPE] = {"movie", "episode", "show"}

_SITEINDEX_SCHEMA: dict[str, pl.PolarsDataType] = {
    "loc": pl.Utf8,
}
_SITEINDEX_DTYPE: pl.PolarsDataType = pl.List(pl.Struct(_SITEINDEX_SCHEMA))


def siteindex(sitemap_type: _TYPE) -> pl.LazyFrame:
    return (
        pl.LazyFrame({"type": [sitemap_type]})
        .select(
            pl.format("https://tv.apple.com/sitemaps_tv_index_{}_1.xml", pl.col("type"))
            .pipe(prepare_request)
            .pipe(
                request,
                session=_APPLETV_SESSION,
                log_group="tv.apple.com/sitemaps_tv_index_type_1.xml",
            )
            .pipe(response_text)
            .pipe(
                xml_extract,
                dtype=_SITEINDEX_DTYPE,
                log_group="parse_siteindex_xml",
            )
            .alias("siteindex"),
        )
        .explode("siteindex")
        .unnest("siteindex")
    )


_SITEMAP_SCHEMA: dict[str, pl.PolarsDataType] = {
    "loc": pl.Utf8,
    "lastmod": pl.Utf8,
    "changefreq": pl.Utf8,
    "priority": pl.Utf8,
}
_SITEMAP_DTYPE: pl.PolarsDataType = pl.List(pl.Struct(_SITEMAP_SCHEMA))


def sitemap(sitemap_type: _TYPE, limit: int | None = None) -> pl.LazyFrame:
    return (
        siteindex(sitemap_type)
        .pipe(head, n=limit)
        .select(
            pl.col("loc")
            .pipe(prepare_request)
            .pipe(
                request,
                session=_APPLETV_SESSION,
                log_group="tv.apple.com/sitemaps_tv_type.xml.gz",
            )
            .struct.field("data")
            .pipe(_zlib_decompress_expr)
            .pipe(
                xml_extract,
                dtype=_SITEMAP_DTYPE,
                log_group="parse_sitemap_xml",
            )
            .alias("sitemap")
        )
        .explode("sitemap")
        .unnest("sitemap")
        .select(
            pl.col("loc"),
            (
                pl.col("lastmod")
                .str.strptime(dtype=pl.Datetime(time_unit="ns"), format="%+")
                .cast(pl.Datetime(time_unit="ns"))
            ),
            pl.col("changefreq").cast(pl.Categorical),
            pl.col("priority").cast(pl.Float32),
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
    )


_LOC_PATTERN = (
    r"https://tv.apple.com/"
    r"(?P<country>[a-z]{2})/"
    r"(?P<type>episode|movie|show)/"
    r"(?P<slug>[^/]*)/"
    r"(?P<id>umc.cmc.[0-9a-z]+)"
)


def url_extract_id(url: pl.Expr) -> pl.Expr:
    return url.str.extract(_LOC_PATTERN, 4)


def cleaned_sitemap(sitemap_type: _TYPE, limit: int | None = None) -> pl.LazyFrame:
    # TODO: str.extract should return a struct
    return (
        sitemap(sitemap_type, limit=limit)
        .with_columns(
            (
                pl.col("loc")
                .str.extract(_LOC_PATTERN, 1)
                .cast(pl.Categorical)
                .alias("country")
            ),
            (
                pl.col("loc")
                .str.extract(_LOC_PATTERN, 2)
                .cast(pl.Categorical)
                .alias("type")
            ),
            (
                pl.col("loc")
                .str.extract(_LOC_PATTERN, 3)
                .pipe(
                    apply_with_tqdm,
                    urllib.parse.unquote,
                    return_dtype=pl.Utf8,
                    log_group="urllib.parse.unquote",
                )
                .alias("slug")
            ),
            pl.col("loc").str.extract(_LOC_PATTERN, 4).alias("id"),
            pl.lit(True).alias("in_latest_sitemap"),
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
        .unique("loc")
    )


_JSONLD_DTYPE = pl.Struct(
    [
        pl.Field("name", pl.Utf8),
        pl.Field("datePublished", pl.Utf8),
        pl.Field("director", pl.List(pl.Struct([pl.Field("name", pl.Utf8)]))),
    ]
)

_JSONLD_SUCCESS_EXPR = pl.col("jsonld").struct.field("name").is_not_null()
_JSONLD_TITLE_EXPR = (
    pl.col("jsonld")
    .struct.field("name")
    .pipe(
        apply_with_tqdm,
        html.unescape,
        return_dtype=pl.Utf8,
        log_group="html.unescape",
    )
)
_JSONLD_PUBLISHED_AT_EXPR = (
    pl.col("jsonld")
    .struct.field("datePublished")
    .str.strptime(dtype=pl.Date, format="%+")
)
_JSONLD_DIRECTOR_EXPR = (
    pl.col("jsonld")
    .struct.field("director")
    .arr.first()
    .struct.field("name")
    .pipe(
        apply_with_tqdm,
        html.unescape,
        return_dtype=pl.Utf8,
        log_group="html.unescape",
    )
)
_RESPONSE_DATE_EXPR = (
    pl.col("response").pipe(response_date).cast(pl.Datetime(time_unit="ns"))
)


def fetch_jsonld_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.with_columns(
            pl.col("loc")
            .pipe(prepare_request, headers=_BROWSER_HEADERS)
            .pipe(request, session=_APPLETV_SESSION, log_group="tv.apple.com")
            .alias("response")
        )
        .with_columns(
            pl.col("response")
            .pipe(response_text)
            .pipe(_extract_jsonld_expr)
            .str.json_extract(dtype=_JSONLD_DTYPE)
            .alias("jsonld")
        )
        .with_columns(
            _JSONLD_SUCCESS_EXPR.alias("jsonld_success"),
            _JSONLD_TITLE_EXPR.alias("title"),
            _JSONLD_PUBLISHED_AT_EXPR.alias("published_at"),
            _JSONLD_DIRECTOR_EXPR.alias("director"),
            _RESPONSE_DATE_EXPR.alias("retrieved_at"),
        )
        .drop(["response", "jsonld"])
    )


def _extract_jsonld_expr(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        _extract_jsonld,
        return_dtype=pl.Utf8,
        log_group="extract_jsonld",
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


def appletv_to_itunes_series(s: pl.Series) -> pl.Series:
    return (
        s.to_frame("id")
        .select(
            pl.format("https://tv.apple.com/us/movie/{}", pl.col("id"))
            .pipe(
                prepare_request,
                headers=_BROWSER_HEADERS,
            )
            .pipe(request, session=_APPLETV_SESSION, log_group="tv.apple.com")
            .pipe(response_text)
            .pipe(
                apply_with_tqdm,
                extract_itunes_id,
                return_dtype=pl.Int64,
                log_group="extract_itunes_id",
            )
            .cast(pl.UInt64),
        )
        .to_series()
        .alias(s.name)
    )


def _extract_shoebox(soup: BeautifulSoup) -> list[Any]:
    script = soup.find("script", {"type": "fastboot/shoebox", "id": "shoebox-uts-api"})
    if not script:
        return []

    return json.loads(script.text).values()


def extract_itunes_id(text: str) -> int | None:
    soup = BeautifulSoup(text, "html.parser")

    if soup.find("h1", string="This content is no longer available."):
        return None

    link = soup.find("link", attrs={"rel": "canonical"})
    if not link:
        return None

    for data in _extract_shoebox(soup):
        if "content" in data and "playables" in data["content"]:
            for playable in data["content"]["playables"]:
                if playable.get("isItunes", False) is True:
                    return int(playable["externalId"])

        if "playables" in data:
            for playable in data["playables"].values():
                if playable["channelId"] == "tvs.sbd.9001":
                    return int(playable["externalId"])

        if "howToWatch" in data:
            for way in data["howToWatch"]:
                if way["channelId"] != "tvs.sbd.9001":
                    continue

                if way.get("punchoutUrls"):
                    m = re.match(
                        r"itmss://itunes.apple.com/us/[^/]+/[^/]+/id(\d+)",
                        way["punchoutUrls"]["open"],
                    )
                    if m:
                        return int(m.group(1))

                if way.get("versions"):
                    for version in way["versions"]:
                        m = re.match(
                            r"tvs.sbd.9001:(\d+)",
                            version["playableId"],
                        )
                        if m:
                            return int(m.group(1))

    return None


_REGIONS = ["us", "gb", "au", "br", "de", "ca", "it", "es", "fr", "jp", "cn"]
REGION_COUNT = len(_REGIONS)


def not_found(df: pl.LazyFrame, sitemap_type: _TYPE) -> pl.LazyFrame:
    return (
        df.lazy()
        .with_columns(
            pl.lit([_REGIONS]).alias("region"),
        )
        .explode("region")
        .with_columns(
            pl.format(
                "https://tv.apple.com/{}/{}/{}",
                pl.col("region"),
                pl.lit(sitemap_type),
                pl.col("id"),
            )
            .pipe(prepare_request, headers=_BROWSER_HEADERS)
            .pipe(request, session=_APPLETV_SESSION, log_group="tv.apple.com")
            .pipe(response_text)
            .str.contains('<div class="not-found">', literal=True)
            .alias("not_found")
        )
        .groupby(*df.columns)
        .agg(
            pl.col("not_found").all().alias("all_not_found"),
        )
    )


_JSONLD_LIMIT = 1_000


def _backfill_jsonld(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.cache()
    df_new = (
        df.filter(pl.col("jsonld_success").is_null())
        .sort(
            (pl.col("country") == "us"),
            pl.col("in_latest_sitemap"),
            pl.col("priority"),
            descending=True,
        )
        .pipe(limit, sample=False, soft=_JSONLD_LIMIT, desc="jsonld")
        .select("loc")
        .pipe(fetch_jsonld_columns)
    )
    return df.pipe(update_or_append, df_new, on="loc").sort(by="loc")


def _fetch_latest_sitemap(df: pl.LazyFrame, sitemap_type: _TYPE) -> pl.LazyFrame:
    latest_sitemap = cleaned_sitemap(sitemap_type)
    return (
        df.with_columns(
            pl.lit(False).alias("in_latest_sitemap"),
        )
        .pipe(update_or_append, latest_sitemap, on="loc")
        .sort(by="loc")
    )


def main() -> None:
    sitemap_type = sys.argv[1]
    assert sitemap_type in _TYPES

    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        df_sitemap = _fetch_latest_sitemap(df, sitemap_type)
        return df.pipe(update_or_append, df_sitemap, on="loc").pipe(_backfill_jsonld)

    with pl.StringCache():
        update_parquet("appletv.parquet", update, key="loc")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
