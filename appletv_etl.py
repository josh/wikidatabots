# pyright: strict

import json
import logging
import re
import sys
from typing import Literal

import polars as pl
from bs4 import BeautifulSoup

from polars_requests import prepare_request, request, response_date, response_text
from polars_utils import (
    apply_with_tqdm,
    assert_expression,
    head,
    html_unescape,
    html_unescape_list,
    limit,
    update_or_append,
    update_parquet,
    xml_extract,
    zlib_decompress,
)

_APPLETV_TIMEOUT = 30.0
_RETRY_COUNT = 10

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
            .pipe(request, log_group="tv.apple.com/sitemaps_tv_index_type_1.xml")
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
            .pipe(request, log_group="tv.apple.com/sitemaps_tv_type.xml.gz")
            .struct.field("data")
            .pipe(zlib_decompress)
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
            pl.col("priority").cast(pl.Float32),
        )
    )


_ID_PATTERN = r"^(umc.cmc.[a-z0-9]{21,25})$"

_LOC_PATTERN = (
    r"^https://tv.apple.com/"
    r"(?P<country>[a-z]{2})/"
    r"(?P<type>episode|movie|show)/"
    r"(?P<slug>[^/]*)/"
    r"(?P<id>umc.cmc.[0-9a-z]{21,25})$"
)

LOC_SHOW_PATTERN = r"showId=(umc.cmc.[0-9a-z]{21,25})$"


def valid_appletv_id(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(_ID_PATTERN)


def url_extract_id(url: pl.Expr) -> pl.Expr:
    return url.str.extract(_LOC_PATTERN, 4)


def cleaned_sitemap(sitemap_type: _TYPE, limit: int | None = None) -> pl.LazyFrame:
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
            pl.col("loc").str.extract(_LOC_PATTERN, 4).alias("id"),
            pl.lit(True).alias("in_latest_sitemap"),
        )
        .select(
            [
                "loc",
                "country",
                "type",
                "id",
                "priority",
                "in_latest_sitemap",
                "lastmod",
            ]
        )
        .pipe(assert_expression, pl.col("id").is_not_null(), "bad sitemap id")
        .unique("loc")
    )


_JSONLD_DTYPE = pl.Struct(
    [
        pl.Field("name", pl.Utf8),
        pl.Field("datePublished", pl.Utf8),
        pl.Field("director", pl.List(pl.Struct([pl.Field("name", pl.Utf8)]))),
    ]
)


def fetch_jsonld_columns(df: pl.LazyFrame) -> pl.LazyFrame:
    return (
        df.with_columns(
            pl.col("loc")
            .pipe(prepare_request, headers=_BROWSER_HEADERS)
            .pipe(
                request,
                log_group="tv.apple.com",
                timeout=10.0,
                retry_count=11,
                bad_statuses={502},
            )
            .alias("response")
        )
        .with_columns(
            pl.col("response").pipe(response_text).pipe(_parse_html_expr).alias("soup"),
        )
        .with_columns(
            (
                pl.col("soup")
                .pipe(
                    _extract_script_text,
                    script_type="application/ld+json",
                    log_group="extract_jsonld",
                )
                .str.json_extract(dtype=_JSONLD_DTYPE)
                .alias("jsonld")
            ),
            (
                pl.col("soup")
                .pipe(
                    _extract_script_text,
                    script_type="fastboot/shoebox",
                    script_id="shoebox-uts-api",
                    log_group="extract_shoebox",
                )
                .pipe(_extract_itunes_id_expr)
                .cast(pl.UInt64)
                .alias("itunes_id")
            ),
        )
        .with_columns(
            (
                pl.col("jsonld")
                .struct.field("name")
                .is_not_null()
                .alias("jsonld_success")
            ),
            pl.col("jsonld").struct.field("name").pipe(html_unescape).alias("title"),
            (
                pl.col("jsonld")
                .struct.field("datePublished")
                .str.strptime(dtype=pl.Date, format="%+")
                .alias("published_at")
            ),
            (
                pl.col("jsonld")
                .struct.field("director")
                .arr.eval(pl.element().struct.field("name"))
                .pipe(html_unescape_list)
                .alias("directors")
            ),
            (
                pl.col("response")
                .pipe(response_date)
                .cast(pl.Datetime(time_unit="ns"))
                .alias("retrieved_at")
            ),
        )
        .select(
            "loc",
            "retrieved_at",
            "jsonld_success",
            "title",
            "published_at",
            "directors",
            "itunes_id",
        )
    )


def _parse_html_expr(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        _parse_html,
        return_dtype=pl.Object,
        log_group="parse_html",
    )


def _parse_html(html: str) -> BeautifulSoup | None:
    soup = BeautifulSoup(html, "html.parser")

    if soup.find("h1", string="This content is no longer available."):
        return None

    link = soup.find("link", attrs={"rel": "canonical"})
    if not link:
        return None

    return soup


def _extract_script_text(
    expr: pl.Expr,
    log_group: str,
    script_type: str | None = None,
    script_id: str | None = None,
) -> pl.Expr:
    attrs = {}
    if script_type:
        attrs["type"] = script_type
    if script_id:
        attrs["id"] = script_id

    def _soup_find_script_text(soup: BeautifulSoup) -> str | None:
        if script := soup.find("script", attrs):
            return script.text
        return None

    return apply_with_tqdm(
        expr,
        _soup_find_script_text,
        return_dtype=pl.Utf8,
        log_group=log_group,
    )


def _extract_itunes_id_expr(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        _extract_itunes_id,
        return_dtype=pl.Int64,
        log_group="extract_itunes_id",
    )


def _extract_itunes_id(text: str) -> int | None:
    for data in json.loads(text).values():
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
                        r"itmss://itunes.apple.com/[a-z]{2}/[^/]+/[^/]+/id(\d+)",
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
            .pipe(
                request,
                log_group="tv.apple.com",
                timeout=_APPLETV_TIMEOUT,
                retry_count=_RETRY_COUNT,
            )
            .pipe(response_text)
            .str.contains('<div class="not-found">', literal=True)
            .alias("not_found")
        )
        .groupby(*df.columns)
        .agg(
            pl.col("not_found").all().alias("all_not_found"),
        )
    )


_JSONLD_LIMIT = 2_500
_OUTDATED_JSONLD = pl.col("country").eq("us") & pl.col("retrieved_at").is_null()


def _backfill_jsonld(df: pl.LazyFrame) -> pl.LazyFrame:
    # MARK: pl.LazyFrame.cache
    df = df.cache()
    df_new = (
        df.filter(_OUTDATED_JSONLD)
        .sort(
            pl.col("in_latest_sitemap"),
            pl.col("priority"),
            descending=True,
        )
        .pipe(limit, _JSONLD_LIMIT, sample=False, desc="jsonld")
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


_GC = pl.col("in_latest_sitemap").is_not() & pl.col("country").ne("us")


def _gc(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(_GC.is_not())


def main() -> None:
    sitemap_type = sys.argv[1]
    assert sitemap_type in _TYPES

    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        df_sitemap = _fetch_latest_sitemap(df, sitemap_type)
        return (
            df.pipe(update_or_append, df_sitemap, on="loc")
            .pipe(_backfill_jsonld)
            .pipe(_gc)
        )

    with pl.StringCache():
        update_parquet("appletv.parquet", update, key="loc")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
