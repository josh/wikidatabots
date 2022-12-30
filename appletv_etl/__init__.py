# pyright: basic

import atexit
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from typing import Literal

import fsspec
import pandas as pd
from dateutil import tz
from pandas._typing import Dtype
from tqdm import tqdm

from pandas_utils import df_upsert

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


def sitemap(type: Type) -> pd.DataFrame:
    index_df = siteindex(type)

    tqdm.pandas(desc=f"Fetching {type} sitemaps")
    ofs = index_df["loc"].progress_apply(cached_fs.open)

    tqdm.pandas(desc=f"Parsing {type} sitemaps")
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
