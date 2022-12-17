# pyright: basic

import os
import sys
from glob import glob

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather

from utils import np_reserve_capacity

changed_json_dirname = sys.argv[2]
changed_json_filenames = sorted(glob("*.json", root_dir=changed_json_dirname))
changed_json_ids = [int(fn.split(".")[0]) for fn in changed_json_filenames]

dfs = []
for idx, fn in enumerate(changed_json_filenames):
    dfx = pd.read_json(os.path.join(changed_json_dirname, fn), lines=True)
    dfx["id"] = changed_json_ids[idx]
    dfs.append(dfx)

df = pd.concat(dfs, ignore_index=True)
df = df.drop(columns=["success", "status_code", "status_message"], errors="ignore")
df["imdb_numeric_id"] = (
    df["imdb_id"].str.removeprefix("tt").str.removeprefix("nm").astype("Int64")
)
df["retrieved_at"] = np.datetime64("now")
print(df, file=sys.stderr)

imdb_ids = np.zeros(0, np.uint32)
tvdb_ids = np.zeros(0, np.uint32)
timestamps = np.empty(0, "datetime64[s]")

filename = sys.argv[1]
table = feather.read_table(filename)
imdb_ids = table.column("imdb_id").to_numpy().copy()
if "tvdb_id" in table.column_names:
    tvdb_ids = table.column("tvdb_id").to_numpy().copy()
timestamps = table.column("retrieved_at").to_numpy().copy()

for row in df.itertuples():
    size = row.id + 1
    imdb_ids = np_reserve_capacity(imdb_ids, size, 0)
    tvdb_ids = np_reserve_capacity(tvdb_ids, size, 0)
    timestamps = np_reserve_capacity(timestamps, size, np.datetime64("nat"))

    if not pd.isna(row.imdb_numeric_id):
        imdb_ids[row.id] = row.imdb_numeric_id

    if "tvdb_id" in df and not pd.isna(row.tvdb_id):
        tvdb_ids[row.id] = row.tvdb_id

    timestamps[row.id] = row.retrieved_at

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
