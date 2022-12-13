import os
import sys
from datetime import date
from glob import glob

import pandas as pd

removed = 0
added = 0

filename_existing = sys.argv[1]
df = pd.read_feather(filename_existing)

root_dir = sys.argv[2]
filenames = sorted(glob("*.arrow", root_dir=root_dir))
dates = [date.fromisoformat(filename.split(".")[0]) for filename in filenames]

rows_to_drop = df[df["date"].isin(dates)].index
removed = len(rows_to_drop)
df = df.drop(rows_to_drop)

dfs = [df]
for idx, filename in enumerate(filenames):
    df_new = pd.read_feather(os.path.join(root_dir, filename))
    df_new["date"] = dates[idx]
    dfs.append(df_new)
    added += len(df_new)


df = pd.concat(dfs)
df = df.sort_values(by=["date"], kind="stable")
df = df.reset_index(drop=True)

print(f"+{added:,}/-{removed:,} rows", file=sys.stderr)
df.to_feather(filename_existing)
