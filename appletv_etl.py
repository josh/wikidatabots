import json
import re
import sys
from typing import Literal

import polars as pl
from bs4 import BeautifulSoup
from polars._typing import PolarsDataType

from polars_requests import prepare_request, request, response_date, response_text
from polars_utils import (
    SomeFrame,
    apply_with_tqdm,
    head,
    html_unescape,
    html_unescape_list,
    lazy_map_reduce_batches,
    map_streaming,
    update_or_append,
    update_parquet,
    xml_extract,
    zlib_decompress,
)

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


def _cast_sitemap_type(sitemap_type: str) -> _TYPE:
    if sitemap_type == "movie":
        return "movie"
    elif sitemap_type == "episode":
        return "episode"
    elif sitemap_type == "show":
        return "show"
    else:
        raise ValueError(f"Invalid sitemap_type: {sitemap_type}")


_SITEINDEX_SCHEMA = pl.Schema(
    {
        "loc": pl.Utf8,
    }
)
_SITEINDEX_DTYPE: PolarsDataType = pl.List(pl.Struct(_SITEINDEX_SCHEMA))


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


_SITEMAP_SCHEMA = pl.Schema(
    {
        "loc": pl.Utf8,
        "lastmod": pl.Utf8,
        "priority": pl.Utf8,
    }
)
_SITEMAP_DTYPE: PolarsDataType = pl.List(pl.Struct(_SITEMAP_SCHEMA))


def sitemap(sitemap_type: _TYPE, limit: int | None = None) -> pl.LazyFrame:
    return (
        siteindex(sitemap_type)
        .pipe(head, n=limit)
        .pipe(
            map_streaming,
            pl.col("loc")
            .pipe(prepare_request)
            .pipe(
                request,
                log_group=f"tv.apple.com/sitemaps_tv_{sitemap_type}.xml.gz",
                timeout=30.0,
                retry_count=3,
            )
            .struct.field("data")
            .pipe(zlib_decompress)
            .pipe(
                xml_extract,
                dtype=_SITEMAP_DTYPE,
                log_group="parse_sitemap_xml",
            )
            .alias("sitemap"),
            return_schema={"sitemap": _SITEMAP_DTYPE},
            chunk_size=50,
        )
        .explode("sitemap")
        .unnest("sitemap")
        .select(
            pl.col("loc"),
            (
                pl.col("lastmod")
                .str.strptime(
                    dtype=pl.Datetime(time_unit="ns"),
                    format="%+",
                )
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


def valid_appletv_id(expr: pl.Expr) -> pl.Expr:
    return expr.str.extract(_ID_PATTERN)


def cleaned_sitemap(sitemap_type: _TYPE, limit: int | None = None) -> pl.LazyFrame:
    return (
        sitemap(sitemap_type, limit=limit)
        .with_columns(
            pl.col("loc").str.extract_groups(_LOC_PATTERN).alias("loc_components")
        )
        .unnest("loc_components")
        .with_columns(
            pl.col("country").cast(pl.Categorical),
            pl.col("type").cast(pl.Categorical),
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
        .unique("loc")
    )


_JSONLD_DTYPE = pl.Struct(
    [
        pl.Field("name", pl.Utf8),
        pl.Field("datePublished", pl.Utf8),
        pl.Field("director", pl.List(pl.Struct([pl.Field("name", pl.Utf8)]))),
    ]
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
    attrs: dict[str, str] = {}
    if script_type:
        attrs["type"] = script_type
    if script_id:
        attrs["id"] = script_id

    def _soup_find_script_text(html: str) -> str | None:
        if soup := _parse_html(html):
            if script := soup.find("script", attrs):
                return script.text
        return None

    return apply_with_tqdm(
        expr,
        _soup_find_script_text,
        return_dtype=pl.Utf8,
        log_group=log_group,
    )


def _extract_itunes_id(text: str) -> int | None:
    root = json.loads(text)
    root_key = next(iter(root.keys()), None)
    if root_key is None:
        return None
    data = root[root_key]

    for playable in data.get("playables", {}).values():
        if playable["isItunes"] is True and playable["channelId"] == "tvs.sbd.9001":
            return int(playable["externalId"])

    for how_to_watch in data.get("howToWatch", []):
        if how_to_watch.get("channelId") == "tvs.sbd.9001":
            for version in how_to_watch["versions"]:
                if m := re.match(r"tvs.sbd.9001:(\d+)", version["playableId"]):
                    return int(m.group(1))

    return None


def _extract_itunes_id_expr(expr: pl.Expr) -> pl.Expr:
    return apply_with_tqdm(
        expr,
        _extract_itunes_id,
        return_dtype=pl.Int64,
        log_group="extract_itunes_id",
    )


def fetch_jsonld_columns(df: SomeFrame) -> SomeFrame:
    return (
        df.with_columns(
            pl.col("loc")
            .pipe(prepare_request, headers=_BROWSER_HEADERS)
            .pipe(
                request,
                log_group="tv.apple.com",
                timeout=10.0,
                retry_count=11,
                ok_statuses={200, 404},
                bad_statuses={502},
            )
            .alias("response")
        )
        .with_columns(
            (
                pl.col("response")
                .pipe(response_text)
                .pipe(
                    _extract_script_text,
                    script_type="application/ld+json",
                    log_group="extract_jsonld",
                )
                .str.json_decode(dtype=_JSONLD_DTYPE)
                .alias("jsonld")
            ),
            (
                pl.col("response")
                .pipe(response_text)
                .pipe(
                    _extract_script_text,
                    script_type="fastboot/shoebox",
                    script_id="shoebox-uts-api-cache",
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
                .list.eval(pl.element().struct.field("name"))
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


_OUTDATED_JSONLD = pl.col("country").eq("us") & pl.col("retrieved_at").is_null()


def _backfill_jsonld(df: pl.LazyFrame) -> pl.LazyFrame:
    def map_function(df: pl.LazyFrame) -> pl.LazyFrame:
        return (
            df.filter(_OUTDATED_JSONLD)
            .sort(pl.col("in_latest_sitemap"), pl.col("priority"), descending=True)
            .select("loc")
            .pipe(fetch_jsonld_columns)
        )

    def reduce_function(df: pl.DataFrame, df_new: pl.DataFrame) -> pl.DataFrame:
        return df.pipe(update_or_append, df_new, on="loc").sort(by="loc")

    return df.pipe(
        lazy_map_reduce_batches,
        map_function=map_function,
        reduce_function=reduce_function,
    )


def _fetch_latest_sitemap(df: pl.LazyFrame, sitemap_type: _TYPE) -> pl.LazyFrame:
    latest_sitemap = cleaned_sitemap(sitemap_type)
    return (
        df.with_columns(
            pl.lit(False).alias("in_latest_sitemap"),
        )
        .pipe(update_or_append, latest_sitemap, on="loc")
        .sort(by="loc")
    )


_GC = pl.col("in_latest_sitemap").not_() & (
    pl.col("country").ne("us") | pl.col("jsonld_success").not_()
)


def _gc(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.filter(_GC.not_())


def _main() -> None:
    pl.enable_string_cache()

    sitemap_type = _cast_sitemap_type(sys.argv[1])

    def update(df: pl.LazyFrame) -> pl.LazyFrame:
        df_sitemap = _fetch_latest_sitemap(df, sitemap_type)
        return (
            df.pipe(update_or_append, df_sitemap, on="loc")
            .pipe(_backfill_jsonld)
            .pipe(_gc)
        )

    update_parquet("appletv.parquet", update, key="loc")


if __name__ == "__main__":
    _main()
