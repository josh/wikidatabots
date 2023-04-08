# pyright: strict

import logging

import polars as pl

import appletv
from constants import APPLE_TV_MOVIE_ID_PID, INSTANCE_OF_PID, ITUNES_MOVIE_ID_PID
from page import blocked_qids
from sparql import fetch_statements, sample_items
from timeout import iter_until_deadline


def main():
    """
    Find Wikidata items that are missing a iTunes movie ID (P6398) but have a
    Apple TV movie ID (P9586).
    """

    qids = sample_items(APPLE_TV_MOVIE_ID_PID, limit=1000)

    allowed_classes = set(_fetch_allowed_classes())
    results = fetch_statements(
        qids, [INSTANCE_OF_PID, ITUNES_MOVIE_ID_PID, APPLE_TV_MOVIE_ID_PID]
    )

    edit_summary = "Add iTunes movie ID via Apple TV movie ID"

    for qid in iter_until_deadline(results):
        item = results[qid]

        if qid in blocked_qids():
            logging.debug(f"{qid} is blocked")
            continue

        if not item.get(INSTANCE_OF_PID) or item.get(ITUNES_MOVIE_ID_PID):
            continue

        instance_of = {v for (_, v) in item[INSTANCE_OF_PID]}
        if instance_of.isdisjoint(allowed_classes):
            continue

        for _statement, value in item.get(APPLE_TV_MOVIE_ID_PID, []):
            id = appletv.id(value)
            if itunes_id := appletv.appletv_to_itunes(id):
                print(
                    f'wd:{qid} wdt:P6398 "{itunes_id}" ; '
                    f'wikidatabots:editSummary "{edit_summary}" . '
                )


def _fetch_allowed_classes() -> list[str]:
    return (
        pl.scan_parquet(
            "s3://wikidatabots/wikidata/property_class_constraints.parquet",
            storage_options={"anon": True},
        )
        .filter(pl.col("numeric_pid") == 6398)
        .select("class_qid")
        .collect()
        .to_series()
        .to_list()
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
