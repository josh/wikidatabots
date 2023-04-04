# pyright: strict

"""
Small API wrapper for submitting QuickStatements batches.
<https://quickstatements.toolforge.org/>
"""


from collections.abc import Iterable

import polars as pl
import requests


class APIError(Exception):
    pass


def import_batch(
    username: str,
    token: str,
    format: str,
    data: str | Iterable[str],
    batchname: str | None = None,
) -> int | None:
    """
    Import and run QuickStatements batch in background.
    Returns a batch ID if successfully enqueued.
    """

    if type(data) is not str:
        data = "\n".join(data)

    # Empty batch
    lines = data.split("\n", 1)
    if len(lines) < 2 or not lines[1]:
        return None

    post_data = {
        "action": "import",
        "submit": "1",
        "username": username,
        "token": token,
        "format": format,
        "data": data,
    }

    if batchname:
        post_data["batchname"] = batchname

    url = "https://quickstatements.toolforge.org/api.php"
    r = requests.post(url, data=post_data)
    r.raise_for_status()

    resp = r.json()
    if resp["status"] == "OK":
        return resp["batch_id"]
    else:
        raise APIError(resp["status"])


def df_to_csv(df: pl.LazyFrame, pid: str, filename: str) -> None:
    if "qid" not in df.schema:
        if "numeric_qid" in df.schema:
            df = df.with_columns(pl.format("Q{}", pl.col("numeric_qid")).alias("qid"))
        elif "item" in df.schema:
            df = df.with_columns(
                pl.col("item")
                .str.replace("http://www.wikidata.org/entity/", "")
                .alias("qid")
            )

    assert df.schema["qid"]
    assert df.schema["value"]
    assert pid.startswith("P")

    df.select(
        pl.col("qid"),
        pl.format('"{}"', pl.col("value")).alias(pid),
    ).collect().write_csv(filename, has_header=True)


if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Submit batch to QuickStatements.")
    parser.add_argument("--username", action="store")
    parser.add_argument("--token", action="store")
    parser.add_argument("--format", action="store", default="csv")
    parser.add_argument("--batchname", action="store")
    args = parser.parse_args()

    batch_id = import_batch(
        username=args.username
        or os.environ.get("QUICKSTATEMENTS_USERNAME")
        or os.environ["WIKIDATA_USERNAME"],
        token=args.token or os.environ["QUICKSTATEMENTS_TOKEN"],
        format=args.format,
        data=sys.stdin.read(),
        batchname=args.batchname,
    )
    if batch_id:
        print(f"https://quickstatements.toolforge.org/#/batch/{batch_id}")
