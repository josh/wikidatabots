# pyright: strict

import datetime
import os

import polars as pl
import requests
from tqdm import tqdm

from polars_utils import align_to_index

session = requests.Session()

ONE_DAY = datetime.timedelta(days=1)


def tmdb_changes(date: datetime.date, tmdb_type: str) -> pl.LazyFrame:
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
        pl.from_dicts(data, schema_overrides={"id": pl.UInt32(), "adult": pl.Boolean()})
        .lazy()
        .unique(subset="id", keep="first")
        .with_column(pl.lit(True).alias("has_changes"))
        .with_column(pl.lit(date).alias("date"))
        .select(["id", "has_changes", "date", "adult"])
    )


def insert_tmdb_latest_changes(df: pl.LazyFrame, tmdb_type: str) -> pl.LazyFrame:
    dates = pl.date_range(
        low=pl.col("date").max() - datetime.timedelta(days=3),
        high=datetime.date.today(),
        interval=ONE_DAY,
        name="date",
    )

    # TODO: Avoid collect
    pbar = tqdm(df.select(dates).collect()["date"], desc="Fetch TMDB changes")
    new_dfs = [tmdb_changes(d, tmdb_type) for d in pbar]

    return (
        pl.concat([df.lazy(), *new_dfs])
        .unique(subset="id", keep="last")
        .pipe(align_to_index, name="id")
        .with_columns(pl.col("date").is_not_null().alias("has_changes"))
        .select(["id", "has_changes", "date", "adult"])
    )


OUTDATED = pl.col("date") >= pl.col("retrieved_at").dt.round("1d")
NEVER_FETCHED = pl.col("retrieved_at").is_null()
MISSING_STATUS = pl.col("success").is_null()


def tmdb_outdated_external_ids(
    latest_changes_df: pl.LazyFrame,
    external_ids_df: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        latest_changes_df.join(external_ids_df, on="id", how="left")
        .sort(pl.col("retrieved_at"), reverse=True)
        .filter(OUTDATED | NEVER_FETCHED | MISSING_STATUS)
        .head(5_000)
        .select(["id"])
    )


EXTRACT_IMDB_TITLE_NUMERIC_ID = (
    pl.col("imdb_id")
    .str.extract(r"tt(\d+)", 1)
    .cast(pl.UInt32)
    .alias("imdb_numeric_id")
)

EXTRACT_IMDB_NAME_NUMERIC_ID = (
    pl.col("imdb_id")
    .str.extract(r"nm(\d+)", 1)
    .cast(pl.UInt32)
    .alias("imdb_numeric_id")
)

EXTRACT_IMDB_NUMERIC_ID = {
    "movie": EXTRACT_IMDB_TITLE_NUMERIC_ID,
    "tv": EXTRACT_IMDB_TITLE_NUMERIC_ID,
    "person": EXTRACT_IMDB_NAME_NUMERIC_ID,
}


def fetch_tmdb_external_ids(tmdb_ids: pl.LazyFrame, tmdb_type: str) -> pl.LazyFrame:
    api_key = os.environ["TMDB_API_KEY"]

    records = []
    # TODO: Avoid collect
    pbar = tqdm(tmdb_ids.collect()["id"], desc="Fetch TMDB external IDs")
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
    return (
        pl.from_dicts(records, schema_overrides=schema)
        .lazy()
        .with_columns(
            [
                pl.col("retrieved_at").dt.round("1s"),
                EXTRACT_IMDB_NUMERIC_ID[tmdb_type],
            ]
        )
    )


def insert_tmdb_external_ids(
    df: pl.LazyFrame,
    tmdb_type: str,
    tmdb_ids: pl.LazyFrame,
) -> pl.LazyFrame:
    return (
        pl.concat([df, fetch_tmdb_external_ids(tmdb_ids, tmdb_type)])
        .unique(subset=["id"], keep="last")
        .pipe(align_to_index, name="id")
    )
