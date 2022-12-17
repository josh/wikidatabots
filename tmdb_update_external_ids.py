# pyright: basic

import os
import sys
from glob import glob

import pandas as pd

df = pd.read_feather(sys.argv[1])
if "id" not in df:
    df = df.reset_index(names=["id"])
df = df.set_index("id")

df["imdb_id"] = df["imdb_id"].astype("Int64")
df.loc[df["imdb_id"] == 0, "imdb_id"] = None

if "tvdb_id" in df:
    df["tvdb_id"] = df["tvdb_id"].astype("Int64")
    df.loc[df["tvdb_id"] == 0, "tvdb_id"] = None

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

changed_df["imdb_id"] = pd.to_numeric(
    changed_df["imdb_id"].str.removeprefix("tt").str.removeprefix("nm"),
    errors="coerce",
).astype("Int64")

changed_df["retrieved_at"] = pd.Timestamp.now().floor("s")

if "tvdb_id" in changed_df:
    changed_df = changed_df[["imdb_id", "tvdb_id", "retrieved_at"]]
else:
    changed_df = changed_df[["imdb_id", "retrieved_at"]]

existing_rows = df.index.isin(changed_df.index)
print(f"Dropping {len(df[existing_rows]):,} existing rows", file=sys.stderr)

df = pd.concat([df[~existing_rows], changed_df])
df = df.sort_index().reset_index(names=["id"])

print(f"Would write {len(df):,} rows", file=sys.stderr)
print(df)
# df.to_feather(sys.argv[1])
