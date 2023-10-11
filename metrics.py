import requests

from polars_utils import compute_raw_stats, scan_s3_parquet_anon


def gauge(name: str, labels: dict[str, str], value: int) -> None:
    assert isinstance(value, int)
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
            gauge(
                "wikimedia_edit_count",
                labels,
                obj["total_edit_count"],
            )
        if "deleted_edit_count" in obj:
            gauge(
                "wikimedia_deleted_edit_count",
                labels,
                obj["deleted_edit_count"],
            )
        if "live_edit_count" in obj:
            gauge(
                "wikimedia_live_edit_count",
                labels,
                obj["live_edit_count"],
            )


def parquet_metrics(filename: str) -> None:
    df = scan_s3_parquet_anon(filename)
    labels = {"filename": filename}
    gauge("wikidatabots_dataframe_row_count", labels, len(df))

    df_stats = compute_raw_stats(df)
    for row in df_stats.iter_rows(named=True):
        labels = {
            "filename": filename,
            "column": row["name"],
            "dtype": row["dtype"],
        }
        if row["null_count"] is not None:
            gauge("polars_dataframe_null_count", labels, row["null_count"])
        if row["true_count"] is not None:
            gauge("polars_dataframe_true_count", labels, row["true_count"])
        if row["false_count"] is not None:
            gauge("polars_dataframe_false_count", labels, row["false_count"])
        if row["is_unique"] is not None:
            gauge("polars_dataframe_unique", labels, int(row["is_unique"]))


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
