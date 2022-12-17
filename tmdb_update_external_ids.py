# pyright: basic

import os
import sys
from glob import glob

import pandas as pd

df = pd.read_feather(sys.argv[1]).set_index("id")

changed_json_dirname = sys.argv[2]
changed_json_filenames = sorted(glob("*.json", root_dir=changed_json_dirname))
changed_json_ids = [int(fn.split(".")[0]) for fn in changed_json_filenames]

changed_dfs = []
for idx, fn in enumerate(changed_json_filenames):
    dfx = pd.read_json(os.path.join(changed_json_dirname, fn), lines=True)
    dfx["id"] = changed_json_ids[idx]
    dfx = dfx.set_index("id")
    changed_dfs.append(dfx)

changed_df = pd.concat(changed_dfs)

changed_df["imdb_numeric_id"] = pd.to_numeric(
    changed_df["imdb_id"].str.removeprefix("tt").str.removeprefix("nm"),
    errors="coerce",
).astype("Int64")

changed_df["retrieved_at"] = pd.Timestamp.now().floor("s")

if "tvdb_id" in changed_df:
    changed_df = changed_df[["imdb_id", "imdb_numeric_id", "tvdb_id", "retrieved_at"]]
else:
    changed_df = changed_df[["imdb_id", "imdb_numeric_id", "retrieved_at"]]

print(changed_df, file=sys.stderr)

existing_rows = df.index.isin(changed_df.index)
print(f"Dropping {len(df[existing_rows]):,} existing rows", file=sys.stderr)

df = pd.concat([df[~existing_rows], changed_df])
df = df.sort_index().reset_index(names=["id"])

print(f"Would write {len(df):,} rows", file=sys.stderr)
print(df, file=sys.stderr)
df.to_feather(sys.argv[1])
