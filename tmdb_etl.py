import datetime
import os
from datetime import timedelta

import pandas as pd
import polars as pl
import requests
from tqdm import tqdm

import actions
from pandas_utils import (
    df_assign_or_append,
    ensure_astype,
    ensure_astypes,
    read_json_series,
    reindex_as_range,
)
from polars_utils import reindex_as_range as reindex_as_range_pl

actions.install_warnings_hook()

session = requests.Session()


def tmdb_changes(date: datetime.date, tmdb_type: str) -> pl.DataFrame:
    start_date = date
    end_date = start_date + timedelta(days=7)
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
        .with_column(pl.lit(date).alias("date"))
        .select(["date", "id", "adult"])
    )


def insert_tmdb_changes(df: pl.DataFrame, tmdb_type: str) -> pl.DataFrame:
    start_date = df["date"].max()
    assert isinstance(start_date, datetime.date)
    dates = pl.date_range(
        low=start_date - datetime.timedelta(days=14),  # days=3
        high=datetime.date.today(),
        interval=datetime.timedelta(days=1),
        name="date",
    )

    pbar = tqdm(dates, desc="Fetch TMDB changes")
    df_new = pl.concat([tmdb_changes(d, tmdb_type) for d in pbar])

    df_old = df.filter(pl.col("date").is_in(dates).is_not())
    return df_old.extend(df_new).sort("date")


def tmdb_latest_changes(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.unique(subset=["id"], keep="last")
        .pipe(reindex_as_range_pl, name="id")
        .with_column(pl.col("date").is_not_null().alias("has_changes"))
        .select(["id", "has_changes", "date", "adult"])
    )


def tmdb_outdated_external_ids(
    latest_changes_df: pd.DataFrame,
    external_ids_df: pd.DataFrame,
) -> pd.Series:
    assert latest_changes_df.index.name == "id", "set index to id"
    assert external_ids_df.index.name == "id", "set index to id"

    df = latest_changes_df.join(external_ids_df, how="left")
    is_missing = df["retrieved_at"].isna()
    is_outdated = df["date"] >= df["retrieved_at"].dt.floor(freq="D")
    df = df[is_missing | is_outdated]
    return ensure_astype(df.index.to_series(), dtype="uint32")


def tmdb_external_ids_need_backfill(external_ids_df: pd.DataFrame) -> pd.Series:
    df = external_ids_df
    assert df.index.name == "id", "set index to id"
    return ensure_astype(df[df["success"].isna()].index.to_series(), dtype="uint32")


EXTERNAL_IDS_DTYPES = {
    "id": "UInt32",
    "imdb_id": "string",
    "tvdb_id": "UInt32",
    "wikidata_id": "string",
    "facebook_id": "string",
    "instagram_id": "string",
    "twitter_id": "string",
    "success": "boolean",
    "status_code": "UInt8",
    "status_message": "string",
}


def fetch_tmdb_external_ids(tmdb_ids: pd.Series, tmdb_type: str) -> pd.DataFrame:
    assert len(tmdb_ids) > 0, "no ids"
    assert tmdb_ids.index.name == "id"
    assert tmdb_ids.index.dtype == "uint64"
    # assert tmdb_ids.equals(tmdb_ids.index.to_series())

    api_key = os.environ["TMDB_API_KEY"]
    urls = (
        "https://api.themoviedb.org/3/"
        + tmdb_type
        + "/"
        + tmdb_ids.astype("string")
        + "/external_ids"
    )
    params = {"api_key": api_key}

    def fetch(url: str):
        r = session.get(url, params=params)
        return r.text

    tqdm.pandas(desc="Fetch TMDB external IDs")
    jsonl = urls.progress_apply(fetch)  # type: ignore

    df = read_json_series(jsonl, dtype=EXTERNAL_IDS_DTYPES)

    if "success" not in df:
        df["success"] = True
    df["success"] = df["success"].fillna(True).astype("boolean")

    if "imdb_id" in df:
        df["imdb_numeric_id"] = pd.to_numeric(
            df["imdb_id"].str.removeprefix("tt").str.removeprefix("nm"),
            errors="coerce",
        ).astype("UInt32")

    df["retrieved_at"] = pd.Timestamp.now().floor("s")

    df = df.replace(to_replace="", value=pd.NA)

    assert df.index.name == "id"
    assert df.index.dtype == "uint64"
    assert df.index.equals(tmdb_ids.index)
    return df


def insert_tmdb_external_ids(
    df: pd.DataFrame,
    tmdb_type: str,
    tmdb_ids: pd.Series,
) -> pd.DataFrame:
    if len(tmdb_ids) == 0:
        return df
    assert tmdb_ids.index.name == "id"
    assert tmdb_ids.index.dtype == "uint64"
    df_orig = df.copy()
    df_updated_rows = fetch_tmdb_external_ids(tmdb_type=tmdb_type, tmdb_ids=tmdb_ids)
    shared_columns = df_orig.columns.intersection(df_updated_rows.columns)
    df = df_assign_or_append(df, df_updated_rows, shared_columns)
    df = reindex_as_range(df)
    assert len(df) >= len(df_orig)
    assert df.columns.equals(df_orig.columns)
    return df


def set_id_index(df: pd.DataFrame) -> pd.DataFrame:
    assert "id" in df.columns, "missing id column"
    df = ensure_astypes(df, dtypes={"id": "uint32"})
    assert isinstance(df.index, pd.RangeIndex), f"current index is {type(df.index)}"

    df = df.set_index("id")

    assert df.index.name == "id"
    assert df.index.dtype == "uint64"

    return df


def unset_id_index(df: pd.DataFrame) -> pd.DataFrame:
    assert "id" not in df.columns, "id column already exists"
    assert df.index.name == "id", f"id name {df.index.name}"
    # assert (
    #     df.index.dtype == "uint32" or df.index.dtype == "uint64"
    # ), f"id index {df.index.dtype}"

    df = df.reset_index()
    id_col = ensure_astype(df["id"], dtype="uint32")
    df = df.drop(columns=["id"])
    df.insert(0, "id", id_col)

    assert isinstance(df.index, pd.RangeIndex)
    assert df["id"].dtype == "uint32"

    return df
