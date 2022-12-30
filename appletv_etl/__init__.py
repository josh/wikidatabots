# pyright: basic

import atexit
import sys
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from typing import Literal

import fsspec
import pandas as pd
from dateutil import tz
from pandas._typing import Dtype
from tqdm import tqdm

import appletv
from pandas_utils import df_upsert

tqdm.pandas()

PACIFIC_TZ = tz.gettz("America/Los_Angeles")


def next_sitemap_updated_at() -> datetime:
    today = date.today()
    d = today + timedelta(days=7 - today.weekday())
    return datetime(d.year, d.month, d.day, tzinfo=PACIFIC_TZ)


def seconds_until_sitemap_updated() -> int:
    now = datetime.now(timezone.utc)
    return round((next_sitemap_updated_at() - now).total_seconds())


cached_fs = fsspec.filesystem(
    "filecache",
    target_protocol="http",
    cache_storage=".cache/appletv_etl/",
    expiry_time=seconds_until_sitemap_updated(),
    same_names=True,
    compression="infer",
)


atexit.register(cached_fs.clear_expired_cache)


SITEINDEX_DTYPE: dict[str, Dtype] = {
    "loc": "string",
    "lastmod": "datetime64[ns]",
}

Type = Literal["episode", "movie", "show"]


class LogGroup:
    def __init__(self, title: str):
        self.title = title

    def __enter__(self):
        print(f"::group::{self.title}", file=sys.stderr)

    def __exit__(self, type, value, traceback):
        print("::endgroup::", file=sys.stderr)

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return wrapper


@LogGroup(title="Fetching siteindex")
def siteindex(type: Type) -> pd.DataFrame:
    url = f"https://tv.apple.com/sitemaps_tv_index_{type}_1.xml"
    with fsspec.open(url) as f:
        df = pd.read_xml(f, dtype=SITEINDEX_DTYPE)  # type: ignore

    assert df.columns.tolist() == ["loc", "lastmod"]
    assert df["loc"].dtype == "string"
    assert df["lastmod"].dtype == "datetime64[ns]"

    return df


SITEMAP_DTYPE: dict[str, Dtype] = {
    "loc": "string",
    "lastmod": "datetime64[ns]",
    "changefreq": "category",
    "priority": "float16",
    "link": "string",
}


@LogGroup(title="Fetching sitemap")
def sitemap(type: Type) -> pd.DataFrame:
    index_df = siteindex(type)

    with LogGroup("Fetching sitemap"):
        ofs = index_df["loc"].progress_apply(cached_fs.open)

    with LogGroup("Parsing sitemap"):
        dfs = ofs.progress_apply(lambda f: pd.read_xml(f, dtype=SITEMAP_DTYPE))

    ofs.apply(lambda f: f.close())

    df = pd.concat(dfs.to_list(), ignore_index=True)

    assert df.columns.tolist() == ["loc", "lastmod", "changefreq", "priority", "link"]
    assert df["loc"].dtype == "string"
    assert df["lastmod"].dtype == "datetime64[ns]"
    assert df["changefreq"].dtype == "category"
    assert df["priority"].dtype == "float16"
    assert df["link"].dtype == "string"

    return df


@LogGroup(title="Clean Sitemap")
def clean_sitemap(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["link"])
    loc_df = (
        df["loc"]
        .str.extract(
            r"https://tv.apple.com/"
            r"(?P<country>[a-z]{2})/"
            r"(?P<type>episode|movie|show)/"
            r"(?P<slug>[^/]*)/"
            r"(?P<id>umc.cmc.[0-9a-z]+)"
        )
        .astype({"country": "category", "type": "category"})
    )
    loc_df["slug"] = loc_df["slug"].apply(urllib.parse.unquote).astype("string")
    df = pd.concat([df, loc_df], axis=1)

    assert df.columns.tolist() == [
        "loc",
        "lastmod",
        "changefreq",
        "priority",
        "country",
        "type",
        "slug",
        "id",
    ]
    assert df["loc"].dtype == "string"
    assert df["lastmod"].dtype == "datetime64[ns]"
    assert df["changefreq"].dtype == "category"
    assert df["priority"].dtype == "float16"
    assert df["country"].dtype == "category"
    assert df["type"].dtype == "category"
    assert df["slug"].dtype == "string"
    assert df["id"].dtype == "string"

    return df


def cleaned_sitemap(type: Type) -> pd.DataFrame:
    return clean_sitemap(sitemap(type))


def append_sitemap_changes(df: pd.DataFrame, latest_df: pd.DataFrame) -> pd.DataFrame:
    df["in_latest_sitemap"] = False
    latest_df["in_latest_sitemap"] = True
    return df_upsert(df, latest_df, on="loc")


JSONLD_DTYPES = {
    "jsonld_success": "boolean",
    "title": "string",
    "published_at": "object",
    "director": "string",
    "retrieved_at": "datetime64[ns]",
}


@LogGroup(title="Fetching JSON-LD")
def fetch_jsonld_df(urls: pd.Series) -> pd.DataFrame:
    records = urls.progress_apply(appletv.fetch_jsonld)
    df = pd.DataFrame.from_records(records)
    df = df.rename(columns={"success": "jsonld_success"})
    df["retrieved_at"] = pd.Timestamp.now().floor("s")
    return df.astype(JSONLD_DTYPES)
