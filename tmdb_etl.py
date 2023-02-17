# pyright: strict

import datetime
import os

import polars as pl
import requests
from tqdm import tqdm

from polars_utils import align_to_index, request_text

session = requests.Session()

ONE_DAY = datetime.timedelta(days=1)

CHANGES_RESULT_DTYPE = pl.Struct(
    [
        pl.Field("id", pl.Int64),
        pl.Field("adult", pl.Boolean),
    ]
)
CHANGES_RESPONSE_DTYPE = pl.Struct([pl.Field("results", pl.List(CHANGES_RESULT_DTYPE))])

CHANGES_SCHEMA = {
    "id": pl.UInt32,
    "has_changes": pl.Boolean,
    "date": pl.Date,
    "adult": pl.Boolean,
}


def tmdb_changes(df: pl.LazyFrame, tmdb_type: str) -> pl.LazyFrame:
    assert df.schema == {"date": pl.Date}
    assert tmdb_type in ["movie", "tv", "person"]

    return (
        df.with_columns(
            pl.format(
                "https://api.themoviedb.org/3/{}/changes"
                "?api_key={}&start_date={}&end_date={}",
                pl.lit(tmdb_type),
                pl.lit(os.environ["TMDB_API_KEY"]),
                pl.col("date"),
                (pl.col("date") + ONE_DAY),
            ).alias("url")
        )
        .select(
            [
                pl.col("date"),
                pl.col("url")
                .map(request_text, return_dtype=pl.Utf8)
                .str.json_extract(dtype=CHANGES_RESPONSE_DTYPE)
                .struct.field("results")
                .arr.reverse()
                .alias("results"),
            ]
        )
        .explode("results")
        .select(
            [
                pl.col("results").struct.field("id").alias("id").cast(pl.UInt32),
                pl.lit(True).alias("has_changes"),
                pl.col("date"),
                pl.col("results").struct.field("adult").alias("adult"),
            ]
        )
    )


def insert_tmdb_latest_changes(df: pl.LazyFrame, tmdb_type: str) -> pl.LazyFrame:
    assert df.schema == CHANGES_SCHEMA
    assert tmdb_type in ["movie", "tv", "person"]

    df = df.cache()
    dates_df = df.select(
        [
            pl.date_range(
                low=pl.col("date").max().alias("start_date") - ONE_DAY,
                high=datetime.date.today(),
                interval="1d",
            ).alias("date")
        ]
    )

    return (
        pl.concat([df, tmdb_changes(dates_df, tmdb_type=tmdb_type)])
        .unique(subset="id", keep="last")
        .pipe(align_to_index, name="id")
        .with_columns(pl.col("has_changes").fill_null(False))
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
        .head(10_000)
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

EXTRACT_WIKIDATA_NUMERIC_ID = (
    pl.col("wikidata_id")
    .str.extract(r"Q(\d+)", 1)
    .cast(pl.UInt32)
    .alias("wikidata_numeric_id")
)


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
        "id": pl.UInt32,
        "success": pl.Boolean,
        "retrieved_at": pl.Datetime(time_unit="ns"),
        "imdb_id": pl.Utf8,
        "tvdb_id": pl.UInt32,
        "wikidata_id": pl.Utf8,
    }
    return (
        pl.from_dicts(records, schema=schema)  # type: ignore
        .lazy()
        .with_columns(
            [
                pl.col("retrieved_at").dt.round("1s"),
                EXTRACT_IMDB_NUMERIC_ID[tmdb_type],
                EXTRACT_WIKIDATA_NUMERIC_ID,
            ]
        )
        .drop(["wikidata_id"])
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


FIND_RESULT_DTYPE = pl.Struct([pl.Field("id", pl.Int64)])
FIND_RESPONSE_DTYPE = pl.Struct(
    [
        pl.Field("movie_results", pl.List(FIND_RESULT_DTYPE)),
        pl.Field("tv_results", pl.List(FIND_RESULT_DTYPE)),
        pl.Field("person_results", pl.List(FIND_RESULT_DTYPE)),
    ]
)


def tmdb_find(tmdb_type: str, external_id_type: str) -> pl.Expr:
    assert tmdb_type in ["movie", "tv", "person"]
    assert external_id_type in ["imdb_id", "tvdb_id"]

    return (
        pl.format(
            "https://api.themoviedb.org/3/find/{}?api_key={}&external_source={}",
            pl.col(external_id_type),
            pl.lit(os.environ["TMDB_API_KEY"]),
            pl.lit(external_id_type),
        )
        .map(request_text, return_dtype=pl.Utf8)
        .str.json_extract(dtype=FIND_RESPONSE_DTYPE)
        .struct.field(f"{tmdb_type}_results")
        .arr.first()
        .struct.field("id")
        .cast(pl.UInt32)
        .alias("tmdb_id")
    )
