import datetime
import os
from datetime import date, timedelta

import pandas as pd
import requests
from tqdm import tqdm

from pandas_utils import safe_row_concat


def check_tmdb_changes_schema(df: pd.DataFrame) -> None:
    assert df.columns.to_list() == ["date", "id", "adult"]
    assert isinstance(df.index, pd.RangeIndex)
    assert df.dtypes["date"] == "object"
    assert df.dtypes["id"] == "uint32"
    assert df.dtypes["adult"] == "boolean"
    assert len(df) > 0


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

    df = pd.DataFrame(data)
    df["date"] = date
    df = df.astype({"id": "uint32", "adult": "boolean"})
    df = df[["date", "id", "adult"]]

    check_tmdb_changes_schema(df)
    return df


def recent_tmdb_changes(tmdb_type: str):
    tqdm.pandas(desc="Fetch TMDB changes")
    dates = pd.date_range(end=date.today(), periods=7, freq="D").to_series().dt.date
    dfs = dates.progress_apply(tmdb_changes, tmdb_type=tmdb_type)  # type: ignore
    df = safe_row_concat(dfs)
    check_tmdb_changes_schema(df)
    return df


def insert_tmdb_changes(df: pd.DataFrame, tmdb_type: str):
    df_orig = df
    df_new = recent_tmdb_changes(tmdb_type=tmdb_type)

    existing_indices = df["date"].isin(df_new["date"])
    df = df[~existing_indices].reset_index(drop=True)

    df = safe_row_concat([df, df_new])
    df = df.sort_values(by=["date"], kind="stable", ignore_index=True)

    check_tmdb_changes_schema(df)
    assert len(df) >= len(df_orig)
    return df
