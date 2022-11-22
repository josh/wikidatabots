# pyright: basic

import datetime
import logging

import numpy as np
import pyarrow as pa
import pyarrow.feather as feather
import tqdm

import tmdb
from utils import np_reserve_capacity


def main(type_: str, filename: str):
    assert type_ in tmdb.object_types
    type: tmdb.ObjectType = type_

    table = feather.read_table(filename)
    timestamps = table.column("changed_at").to_numpy().copy()

    pbar = tqdm.tqdm(list(reversed(range(3))))
    for n in pbar:
        start_date = datetime.date.today() - datetime.timedelta(days=n + 1)
        end_date = datetime.date.today() - datetime.timedelta(days=n)
        today = np.datetime64(start_date.isoformat(), "D")
        pbar.set_description(f"{today}")

        tmdb_ids = tmdb.changes(
            type=type,
            start_date=start_date,
            end_date=end_date,
        )
        assert len(tmdb_ids) > 0
        for tmdb_id in tmdb_ids:
            size = tmdb_id + 1
            timestamps = np_reserve_capacity(timestamps, size, np.datetime64("nat"))
            timestamps[tmdb_id] = today

    table = pa.table([timestamps], names=["changed_at"])
    feather.write_feather(table, filename)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    main(sys.argv[1], sys.argv[2])
