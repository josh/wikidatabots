# pyright: basic

import json
import os
import sys
from datetime import date
from glob import glob

import pandas as pd

df = input_df = pd.read_feather(sys.argv[1])

root_dir = sys.argv[2]
filenames = sorted(glob("*.json", root_dir=root_dir))
dates = [date.fromisoformat(filename.split(".")[0]) for filename in filenames]

rows_to_drop = df[df["date"].isin(dates)].index
removed = len(rows_to_drop)
df = df.drop(rows_to_drop)

added = 0
dfs = [df]
for idx, filename in enumerate(filenames):
    with open(os.path.join(root_dir, filename), "r") as f:
        data = json.load(f)
    df_new = pd.DataFrame.from_records(data["results"])
    df_new["date"] = dates[idx]
    dfs.append(df_new)
    added += len(df_new)

df = pd.concat(dfs)
df = df.sort_values(by=["date"], kind="stable")
df = df.reset_index(drop=True)

print(f"{added:,}/-{removed:,} rows", file=sys.stderr)

assert (
    df.columns.tolist() == input_df.columns.tolist()
), f"{df.columns} != {input_df.columns}"
assert (
    df.dtypes.tolist() == input_df.dtypes.tolist()
), f"{df.dtypes} != {input_df.dtypes}"
df.to_feather(sys.argv[1])
