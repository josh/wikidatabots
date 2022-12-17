# pyright: basic

import logging
from glob import glob

import numpy as np
import pyarrow as pa
import pyarrow.feather as feather
import tqdm

import imdb
import tmdb
from utils import np_reserve_capacity


def main(
    type_: str,
    filename: str,
    changed_ids_filename: str,
    changed_json_dirname: str,
):
    assert type_ in tmdb.object_types
    type: tmdb.ObjectType = type_
    imdb_type: imdb.IMDBIDType = tmdb.TMDB_TYPE_TO_IMDB_TYPE[type]

    with open(changed_ids_filename) as f:
        changed_ids_next: set[int] = set(map(int, f.readlines()))

    changed_json_filenames = sorted(glob("*.json", root_dir=changed_json_dirname))
    changed_json_ids = [int(fn.split(".")[0]) for fn in changed_json_filenames]

    print("changed_json_filenames:", changed_json_filenames, file=sys.stderr)
    print("changed_json_ids:", changed_json_ids, file=sys.stderr)

    imdb_ids = np.zeros(0, np.uint32)
    tvdb_ids = np.zeros(0, np.uint32)
    timestamps = np.empty(0, "datetime64[s]")

    table = feather.read_table(filename)
    imdb_ids = table.column("imdb_id").to_numpy().copy()
    if "tvdb_id" in table.column_names:
        tvdb_ids = table.column("tvdb_id").to_numpy().copy()
    timestamps = table.column("retrieved_at").to_numpy().copy()

    for tmdb_id in tqdm.tqdm(changed_ids_next):
        size = tmdb_id + 1
        imdb_ids = np_reserve_capacity(imdb_ids, size, 0)
        tvdb_ids = np_reserve_capacity(tvdb_ids, size, 0)
        timestamps = np_reserve_capacity(timestamps, size, np.datetime64("nat"))

        ids = tmdb.external_ids(tmdb_id, type=type)
        if imdb_id := ids.get("imdb_id"):
            imdb_ids[tmdb_id] = imdb.decode_numeric_id(imdb_id, imdb_type) or 0

        tvdb_ids[tmdb_id] = ids.get("tvdb_id") or 0
        timestamps[tmdb_id] = np.datetime64("now")

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
    feather.write_feather(table, filename)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
