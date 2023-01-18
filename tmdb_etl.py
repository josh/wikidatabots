import datetime
import logging
import os
import warnings
from datetime import date, timedelta

import pandas as pd
import requests
from tqdm import tqdm

import actions
from pandas_utils import (
    compact_dtypes,
    df_diff,
    read_json_series,
    reindex_as_range,
    safe_row_concat,
)

actions.install_warnings_hook()

session = requests.Session()


def check_tmdb_changes_schema(df: pd.DataFrame) -> None:
    assert df.columns.to_list() == ["date", "id", "adult"], f"columns are {df.columns}"
    assert isinstance(df.index, pd.RangeIndex), f"index is {type(df.index)}"
    assert df.dtypes["date"] == "object", f"id date is {df.dtypes['date']}"
    assert df.dtypes["id"] == "uint32", f"id dtype is {df.dtypes['id']}"
    assert df.dtypes["adult"] == "boolean", f"adult dtype is {df.dtypes['adult']}"
    assert len(df) > 0, "empty dataframe"
    assert df["date"].is_monotonic_increasing, "dates are not sorted"


def check_tmdb_external_ids_schema(df: pd.DataFrame) -> None:
    assert df.dtypes["id"] == "uint32", f"id dtype is {df.dtypes['id']}"
    assert df.dtypes["imdb_id"] == "string", f"imdb_id dtype is {df.dtypes['imdb_id']}"

    assert (
        df.dtypes["imdb_numeric_id"] == "UInt32"
    ), f"imdb_numeric_id dtype is {df.dtypes['imdb_numeric_id']}"

    assert (
        df.dtypes["wikidata_id"] == "string"
    ), f"wikidata_id dtype is {df.dtypes['wikidata_id']}"

    if "tvdb_id" in df.columns:
        assert (
            df.dtypes["tvdb_id"] == "UInt32"
        ), f"tvdb_id dtype is {df.dtypes['tvdb_id']}"

    assert len(df) > 0, "empty dataframe"


def tmdb_changes(date: date, tmdb_type: str) -> pd.DataFrame:
    start_date = date
    end_date = start_date + timedelta(days=1)
    api_key = os.environ["TMDB_API_KEY"]
    assert type(start_date) == datetime.date, f"start_date: {type(start_date)}"
    assert type(end_date) == datetime.date, f"end_date: {type(end_date)}"

    url = f"https://api.themoviedb.org/3/{tmdb_type}/changes"
    params = {
        "api_key": api_key,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    r = session.get(url, params=params)
    data = r.json()["results"]

    df = pd.DataFrame(data).pipe(compact_dtypes)
    df["date"] = date
    df = df[["date", "id", "adult"]]

    logging.debug(f"{len(df)} changes on {date}")
    check_tmdb_changes_schema(df)
    return df


def recent_tmdb_changes(start_date: date, tmdb_type: str):
    assert type(start_date) == date, f"start_date: {type(start_date)}"
    start = start_date - timedelta(days=7)
    end = date.today()
    dates = pd.date_range(start=start, end=end, freq="D").to_series().dt.date

    tqdm.pandas(desc="Fetch TMDB changes")
    dfs = dates.progress_apply(tmdb_changes, tmdb_type=tmdb_type)  # type: ignore
    df = safe_row_concat(dfs)

    check_tmdb_changes_schema(df)
    return df


def xinsert_tmdb_changes(df: pd.DataFrame, tmdb_type: str):
    initial_size = len(df)
    df_new = recent_tmdb_changes(start_date=df["date"].max(), tmdb_type=tmdb_type)

    existing_indices = df["date"].isin(df_new["date"])
    df = df[~existing_indices].reset_index(drop=True)

    df = safe_row_concat([df, df_new])

    check_tmdb_changes_schema(df)
    if len(df) < initial_size:
        warnings.warn(f"before {initial_size}, after {len(df)}")
    return df


def insert_tmdb_changes(df: pd.DataFrame, tmdb_type: str):
    start_date = df["date"].max()
    assert type(start_date) == datetime.date

    start = start_date - timedelta(days=7)
    end = datetime.date.today()
    dates = pd.date_range(start=start, end=end, freq="D").to_series().dt.date

    for d in dates:
        df_prev = df.copy()
        df_new = tmdb_changes(d, tmdb_type=tmdb_type)

        existing_indices = df["date"] == d
        df_existing = df[existing_indices]
        df = df[~existing_indices].reset_index(drop=True)

        if len(df_new) < len(df_existing):
            warnings.warn(f"{d}: before {len(df_existing)}, after {len(df_new)}")

        df = safe_row_concat([df, df_new])
        (added, removed, _) = df_diff(df_prev, df)
        print(f"{d}: +{added:,} -{removed:,}")

    check_tmdb_changes_schema(df)

    return df


def tmdb_latest_changes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["id"], keep="last")
    df = df.set_index("id").pipe(reindex_as_range).sort_index()
    df = df.reset_index().astype({"id": "uint32"})
    df = df.assign(has_changes=df["date"].notna())
    df = df[["id", "has_changes", "date", "adult"]]
    return df


def tmdb_outdated_external_ids(
    latest_changes_df: pd.DataFrame,
    external_ids_df: pd.DataFrame,
) -> pd.Series:
    assert latest_changes_df.index.name == "id", "set index to id"
    assert external_ids_df.index.name == "id", "set index to id"

    df = external_ids_df.join(latest_changes_df, how="left")
    is_missing = df["retrieved_at"].isna()
    is_outdated = df["date"] >= df["retrieved_at"].dt.floor(freq="D")
    df = df[is_missing | is_outdated].reset_index()
    return df["id"]


def tmdb_external_ids_need_backfill(external_ids_df: pd.DataFrame) -> pd.Series:
    assert external_ids_df.index.name == "id", "set index to id"
    df = external_ids_df
    df = df[df["success"].isna()].reset_index()
    return df["id"].head(10_000)


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
    assert isinstance(tmdb_ids.index, pd.RangeIndex), f"index is {type(tmdb_ids.index)}"

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

    df["id"] = tmdb_ids
    df = df.set_index("id")

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

    return df


def insert_tmdb_external_ids(
    df: pd.DataFrame, tmdb_type: str, tmdb_ids: pd.Series
) -> pd.DataFrame:
    if len(tmdb_ids) == 0:
        return df
    df_orig = df.copy()
    df_updated_rows = fetch_tmdb_external_ids(tmdb_type=tmdb_type, tmdb_ids=tmdb_ids)
    assert df_updated_rows.index.isin(df.index).all()
    shared_columns = df_orig.columns.intersection(df_updated_rows.columns)
    df.loc[df_updated_rows.index, shared_columns] = df_updated_rows[shared_columns]
    assert len(df) == len(df_orig)
    assert df.columns.equals(df_orig.columns)
    return df