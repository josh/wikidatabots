import datetime
import logging

import numpy as np
import pyarrow as pa
import pyarrow.feather as feather
import tqdm

import imdb
import tmdb
from utils import np_reserve_capacity

TYPE_TO_IMDB_TYPE: dict[tmdb.ObjectType, imdb.IMDBIDType] = {
    "movie": "tt",
    "tv": "tt",
    "person": "nm",
}


def main(type_: str, filename: str):
    assert type_ in tmdb.object_types
    type: tmdb.ObjectType = type_
    imdb_type: imdb.IMDBIDType = TYPE_TO_IMDB_TYPE[type]

    imdb_ids = np.zeros(0, np.uint32)
    tvdb_ids = np.zeros(0, np.uint32)
    timestamps = np.empty(0, "datetime64[s]")

    try:
        table = feather.read_table(filename)
        logging.info(f"Loaded {table}")
    except FileNotFoundError:
        logging.warning(f"File not found: {filename}")
    else:
        imdb_ids = table.column("imdb_id").to_numpy().copy()
        if "tvdb_id" in table.column_names:
            tvdb_ids = table.column("tvdb_id").to_numpy().copy()
        timestamps = table.column("retrieved_at").to_numpy().copy()

    assert imdb_ids.dtype == np.uint32
    assert tvdb_ids.dtype == np.uint32
    assert timestamps.dtype == "datetime64[s]"
    assert imdb_ids.flags["WRITEABLE"]
    assert tvdb_ids.flags["WRITEABLE"]
    assert timestamps.flags["WRITEABLE"]

    def generate_stats() -> str:
        stats = "non-zero counts:\n"
        stats += f"imdb_id: {np.count_nonzero(imdb_ids)}/{imdb_ids.size}\n"
        if np.any(tvdb_ids):
            stats += f"tvdb_id: {np.count_nonzero(tvdb_ids)}/{tvdb_ids.size}\n"
        stats += (
            f"timestamps: {np.count_nonzero(~np.isnan(timestamps))}/{timestamps.size}"
        )
        return stats

    logging.info(generate_stats())

    start_date = datetime.date.today() - datetime.timedelta(days=3)
    changed_ids = tmdb.changes(type, start_date=start_date)

    for tmdb_id in tqdm.tqdm(changed_ids):
        size = tmdb_id + 1
        imdb_ids = np_reserve_capacity(imdb_ids, size, 0)
        tvdb_ids = np_reserve_capacity(tvdb_ids, size, 0)
        timestamps = np_reserve_capacity(timestamps, size, np.datetime64("nat"))

        ids = tmdb.external_ids(tmdb_id, type=type)
        imdb_id = ids.get("imdb_id")
        if imdb_id:
            imdb_ids[tmdb_id] = imdb.decode_numeric_id(imdb_id, imdb_type) or 0

        tvdb_ids[tmdb_id] = ids.get("tvdb_id") or 0

        timestamps[tmdb_id] = np.datetime64("now")

    logging.info(generate_stats())

    cols = []
    names = []

    names.append("imdb_id")
    cols.append(imdb_ids)

    if np.any(tvdb_ids):
        names.append("tvdb_id")
        cols.append(tvdb_ids)

    names.append("retrieved_at")
    cols.append(timestamps)

    table = pa.table(cols, names=names)
    logging.info(f"Saving {table}")
    feather.write_feather(table, filename)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    main(sys.argv[1], sys.argv[2])
