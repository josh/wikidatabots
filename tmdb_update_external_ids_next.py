# pyright: basic

import logging

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
import tqdm

import imdb
import tmdb
from utils import np_reserve_capacity


def main(type_: str, changes_filename: str, external_ids_filename: str):
    assert type_ in tmdb.object_types
    type: tmdb.ObjectType = type_
    imdb_type: imdb.IMDBIDType = tmdb.TMDB_TYPE_TO_IMDB_TYPE[type]

    # TODO: Precompute `latest_changes_df` data frame
    changes_table = pd.read_feather(changes_filename)
    id_size = changes_table["id"].max() + 1
    latest_changes_df = changes_table.groupby("id").tail(1).set_index("id")
    ids_df = pd.DataFrame({"id": range(id_size)}).set_index("id")
    latest_changes_df = latest_changes_df.join(ids_df, how="right", sort=True)
    changed_ats = latest_changes_df["date"].to_numpy()

    external_ids_table = feather.read_table(external_ids_filename)
    imdb_ids = external_ids_table.column("imdb_id").to_numpy().copy()
    if "tvdb_id" in external_ids_table.column_names:
        tvdb_ids = external_ids_table.column("tvdb_id").to_numpy().copy()
    else:
        tvdb_ids = np.zeros(0, np.uint32)
    retrieved_ats = external_ids_table.column("retrieved_at").to_numpy().copy()

    size = max(len(changed_ats), external_ids_table.num_rows)
    changed_ats = np_reserve_capacity(changed_ats, size, np.datetime64("nat"))
    retrieved_ats = np_reserve_capacity(retrieved_ats, size, np.datetime64("nat"))
    imdb_ids = np_reserve_capacity(imdb_ids, size, 0)
    tvdb_ids = np_reserve_capacity(tvdb_ids, size, 0)

    df = pd.DataFrame(data={"changed_at": changed_ats, "retrieved_at": retrieved_ats})
    changed_tmdb_ids = df[df["changed_at"] >= df["retrieved_at"]].index

    for tmdb_id in tqdm.tqdm(changed_tmdb_ids):
        ids = tmdb.external_ids(tmdb_id, type=type)
        if imdb_id := ids.get("imdb_id"):
            imdb_ids[tmdb_id] = imdb.decode_numeric_id(imdb_id, imdb_type) or 0

        tvdb_ids[tmdb_id] = ids.get("tvdb_id") or 0
        retrieved_ats[tmdb_id] = np.datetime64("now")

    cols = []
    names = []

    names.append("imdb_id")
    cols.append(imdb_ids)

    if np.any(tvdb_ids):
        names.append("tvdb_id")
        cols.append(tvdb_ids)

    names.append("retrieved_at")
    cols.append(retrieved_ats)

    table = pa.table(cols, names=names)
    print("Would write", table, "to", external_ids_filename)
    # feather.write_feather(table, external_ids_filename)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    main(*sys.argv[1:])
