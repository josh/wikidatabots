# pyright: strict

import datetime
import os

import polars as pl
import requests
from tqdm import tqdm

from polars_utils import reindex_as_range

session = requests.Session()

ONE_DAY = datetime.timedelta(days=1)


def tmdb_changes(date: datetime.date, tmdb_type: str) -> pl.DataFrame:
    start_date = date
    end_date = start_date + ONE_DAY
    api_key = os.environ["TMDB_API_KEY"]

    url = f"https://api.themoviedb.org/3/{tmdb_type}/changes"
    params = {
        "api_key": api_key,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    r = session.get(url, params=params)
    data = r.json()["results"]

    return (
        pl.from_dicts(data, schema={"id": pl.UInt32(), "adult": pl.Boolean()})
        .unique(subset="id", keep="first")
        .with_column(pl.lit(True).alias("has_changes"))
        .with_column(pl.lit(date).alias("date"))
        .select(["id", "has_changes", "date", "adult"])
    )


def insert_tmdb_latest_changes(df: pl.DataFrame, tmdb_type: str) -> pl.DataFrame:
    start_date = df["date"].max()
    assert isinstance(start_date, datetime.date)

    dates = pl.date_range(
        low=start_date - datetime.timedelta(days=3),
        high=datetime.date.today(),
        interval=ONE_DAY,
        name="date",
    )

    pbar = tqdm(dates, desc="Fetch TMDB changes")
    return (
        pl.concat([df] + [tmdb_changes(d, tmdb_type) for d in pbar])
        .unique(subset="id", keep="last")
        .pipe(reindex_as_range, name="id")
        .with_columns(pl.col("date").is_not_null().alias("has_changes"))
        .select(["id", "has_changes", "date", "adult"])
    )


def tmdb_outdated_external_ids(
    latest_changes_df: pl.DataFrame,
    external_ids_df: pl.DataFrame,
) -> pl.Series:
    df = latest_changes_df.join(external_ids_df, on="id", how="left")
    is_missing = pl.col("retrieved_at").is_null()
    is_outdated = pl.col("date") >= pl.col("retrieved_at").dt.round("1d")
    return df.filter(is_missing | is_outdated)["id"].head(1_000)


def tmdb_external_ids_need_backfill(external_ids_df: pl.DataFrame) -> pl.Series:
    df = external_ids_df
    return df.filter(df["success"].is_null())["id"].head(1_000)


ExtractIMDbNumericIDExpr = (
    pl.col("imdb_id")
    .str.extract(r"(tt|nm)(\d+)", 2)
    .cast(pl.UInt32)
    .alias("imdb_numeric_id")
)


def fetch_tmdb_external_ids(tmdb_ids: pl.Series, tmdb_type: str) -> pl.DataFrame:
    api_key = os.environ["TMDB_API_KEY"]

    records = []
    pbar = tqdm(tmdb_ids, desc="Fetch TMDB external IDs")
    for tmdb_id in pbar:
        url = f"https://api.themoviedb.org/3/{tmdb_type}/{tmdb_id}/external_ids"
        r = session.get(url, params={"api_key": api_key})
        data = r.json()
        record = {
            "id": tmdb_id,
            "success": data.get("success", True),
            "retrieved_at": datetime.datetime.now(),
            "imdb_id": data.get("imdb_id"),
            "tvdb_id": data.get("tvdb_id"),
            "wikidata_id": data.get("wikidata_id"),
        }
        records.append(record)  # type: ignore

    schema = {
        "id": pl.UInt32(),
        "success": pl.Boolean(),
        "retrieved_at": pl.Datetime(time_unit="ns"),
        "imdb_id": pl.Utf8(),
        "tvdb_id": pl.UInt32(),
        "wikidata_id": pl.Utf8(),
    }
    return pl.from_dicts(records, schema=schema).with_columns(
        [
            pl.col("retrieved_at").dt.round("1s"),
            ExtractIMDbNumericIDExpr,
        ]
    )


def insert_tmdb_external_ids(
    df: pl.DataFrame,
    tmdb_type: str,
    tmdb_ids: pl.Series,
) -> pl.DataFrame:
    # FIXME
    if len(tmdb_ids) == 0:
        return df

    if "tvdb_id" not in df.columns:
        df = df.with_column(pl.lit(None, dtype=pl.UInt32).alias("tvdb_id"))
    df = df.select(
        [
            "id",
            "success",
            "retrieved_at",
            "imdb_id",
            "tvdb_id",
            "wikidata_id",
            "imdb_numeric_id",
        ]
    )

    df_updated_rows = fetch_tmdb_external_ids(tmdb_type=tmdb_type, tmdb_ids=tmdb_ids)
    return (
        df.extend(df_updated_rows)
        .unique(subset=["id"], keep="last")
        .pipe(reindex_as_range, name="id")
    )
