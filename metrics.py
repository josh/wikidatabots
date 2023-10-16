from typing import Literal
import requests

from polars_utils import compute_raw_stats, scan_s3_parquet_anon

_PRINTED_HELP: set[str] = set()
_PRINTED_TYPE: set[str] = set()


def metric(
    name: str,
    labels: dict[str, str],
    value: int,
    help: str,
    type: Literal["gauge", "counter", "histogram", "summary"] = "gauge",
) -> None:
    assert isinstance(value, int)

    if name not in _PRINTED_HELP:
        print(f"# HELP {name} {help}")
        _PRINTED_HELP.add(name)

    if name not in _PRINTED_TYPE:
        print(f"# TYPE {name} {type}")
        _PRINTED_TYPE.add(name)

    line = f"{name}"
    line += "{"
    line += ", ".join([f'{k}="{v}"' for k, v in labels.items()])
    line += "}"
    line += f" {value}"
    print(line)


def xtool_metrics() -> None:
    users = ["Josh404", "Josh404Bot"]

    for user in users:
        url = (
            "https://xtools.wmcloud.org/"
            f"api/user/simple_editcount/www.wikidata.org/{user}"
        )
        r = requests.get(url)
        r.raise_for_status()
        obj = r.json()

        labels = {
            "project": obj["project"],
            "username": obj["username"],
            "user_id": obj["user_id"],
        }

        if "total_edit_count" in obj:
            metric(
                "wikimedia_edits_total",
                labels | {"deleted": "all"},
                obj["total_edit_count"],
                type="counter",
                help="The number of Wikimedia edits",
            )
        if "deleted_edit_count" in obj:
            metric(
                "wikimedia_edits_total",
                labels | {"deleted": "deleted"},
                obj["deleted_edit_count"],
                type="counter",
                help="The number of Wikimedia edits",
            )
        if "live_edit_count" in obj:
            metric(
                "wikimedia_edits_total",
                labels | {"deleted": "live"},
                obj["live_edit_count"],
                type="counter",
                help="The number of Wikimedia edits",
            )


def parquet_metrics(filename: str) -> None:
    df = scan_s3_parquet_anon(filename).collect()
    labels = {"filename": filename}

    metric(
        "wikidatabots_dataframe_rows_total",
        labels,
        len(df),
        help="The number of rows in the Data Frame",
    )

    df_stats = compute_raw_stats(df)
    for row in df_stats.iter_rows(named=True):
        labels = {
            "filename": filename,
            "column": row["name"],
            "dtype": row["dtype"],
        }
        if row["null_count"] is not None:
            metric(
                "polars_dataframe_null_values_total",
                labels,
                row["null_count"],
                help="The number of null values in the column",
            )
        if row["true_count"] is not None:
            metric(
                "polars_dataframe_true_values_total",
                labels,
                row["true_count"],
                help="The number of true values in the column",
            )
        if row["false_count"] is not None:
            metric(
                "polars_dataframe_false_values_total",
                labels,
                row["false_count"],
                help="The number of false values in the column",
            )
        if row["is_unique"] is not None:
            metric(
                "polars_dataframe_unique_values_total",
                labels,
                int(row["is_unique"]),
                help="The number of unique values in the column",
            )


if __name__ == "__main__":
    xtool_metrics()

    parquet_metrics("s3://wikidatabots/itunes.parquet")
    parquet_metrics("s3://wikidatabots/opencritic.parquet")
    parquet_metrics("s3://wikidatabots/plex.parquet")
    parquet_metrics("s3://wikidatabots/tmdb/movie.parquet")
    parquet_metrics("s3://wikidatabots/tmdb/tv.parquet")
    parquet_metrics("s3://wikidatabots/tmdb/person.parquet")
    parquet_metrics("s3://wikidatabots/appletv/movie.parquet")
    parquet_metrics("s3://wikidatabots/appletv/show.parquet")
