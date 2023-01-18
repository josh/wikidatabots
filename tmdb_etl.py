import datetime
import logging
import os
from datetime import date, timedelta

import pandas as pd
import requests
from tqdm import tqdm

from pandas_utils import compact_dtypes, safe_row_concat


def check_tmdb_changes_schema(df: pd.DataFrame) -> None:
    assert df.columns.to_list() == ["date", "id", "adult"], f"columns are {df.columns}"
    assert isinstance(df.index, pd.RangeIndex), f"index is {type(df.index)}"
    assert df.dtypes["date"] == "object", f"id date is {df.dtypes['date']}"
    assert df.dtypes["id"] == "uint32", f"id dtype is {df.dtypes['id']}"
    assert df.dtypes["adult"] == "boolean", f"adult dtype is {df.dtypes['adult']}"
    assert len(df) > 0, "empty dataframe"
    assert df["date"].is_monotonic_increasing, "dates are not sorted"


def tmdb_changes(date: date, tmdb_type: str) -> pd.DataFrame:
    start_date = date
    end_date = start_date + timedelta(days=1)
    api_key = os.environ["TMDB_API_KEY"]
    assert type(start_date) == datetime.date, f"start_date: {type(start_date)}"
    assert type(end_date) == datetime.date, f"end_date: {type(end_date)}"

    url = f"https://api.themoviedb.org/3/{tmdb_type}/changes"
    params = {"api_key": api_key, "start_date": start_date, "end_date": end_date}
    r = requests.get(url, params=params)
    data = r.json()["results"]

    df = pd.DataFrame(data).pipe(compact_dtypes)
    df["date"] = date
    df = df[["date", "id", "adult"]]

    logging.debug(f"{len(df)} changes on {date}")
    check_tmdb_changes_schema(df)
    return df


def recent_tmdb_changes(start_date: date, tmdb_type: str):
    assert type(start_date) == date, f"start_date: {type(start_date)}"
    start = start_date - timedelta(days=2)
    end = date.today()
    dates = pd.date_range(start=start, end=end, freq="D").to_series().dt.date

    tqdm.pandas(desc="Fetch TMDB changes")
    dfs = dates.progress_apply(tmdb_changes, tmdb_type=tmdb_type)  # type: ignore
    df = safe_row_concat(dfs)

    check_tmdb_changes_schema(df)
    return df


def insert_tmdb_changes(df: pd.DataFrame, tmdb_type: str):
    initial_size = len(df)
    df_new = recent_tmdb_changes(start_date=df["date"].max(), tmdb_type=tmdb_type)

    existing_indices = df["date"].isin(df_new["date"])
    df = df[~existing_indices].reset_index(drop=True)

    df = safe_row_concat([df, df_new])

    check_tmdb_changes_schema(df)
    assert len(df) >= initial_size
    return df
