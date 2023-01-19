import datetime
import logging
import os
from datetime import date, timedelta

import pandas as pd
import requests
from tqdm import tqdm

import actions
from pandas_utils import (
    df_assign_or_append,
    ensure_astype,
    ensure_astypes,
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
    assert df.index.name == "id", f"index name was {df.index.name}"
    assert df.index.dtype == "uint64", f"id index dtype is {df.index.dtype}"
    assert df.index.is_monotonic_increasing, "index is not sorted"
    assert df.index.is_unique, "index is not unique"

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

    df = pd.DataFrame(data)
    df = df.astype({"id": "uint32", "adult": "boolean"})
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


def insert_tmdb_changes(df: pd.DataFrame, tmdb_type: str):
    df_new = recent_tmdb_changes(start_date=df["date"].max(), tmdb_type=tmdb_type)
    existing_indices = df["date"].isin(df_new["date"])
    df = df[~existing_indices].reset_index(drop=True)
    df = safe_row_concat([df, df_new])
    check_tmdb_changes_schema(df)
    return df


def tmdb_latest_changes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["id"], keep="last")
    df = df.set_index("id").pipe(reindex_as_range)
    df = unset_id_index(df)
    df = df.assign(has_changes=df["date"].notna())
    df = df[["id", "has_changes", "date", "adult"]]
    return df


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
