import os
import sys
from datetime import date

import pandas as pd

filename_existing = sys.argv[1]
df_existing = pd.read_feather(filename_existing)

filename_new = sys.argv[2]
new_datestar = os.path.basename(filename_new).removesuffix(".arrow")
new_date = date.fromisoformat(new_datestar)
df_new = pd.read_feather(filename_new)
df_new["date"] = new_date

rows_to_drop = df_existing[df_existing["date"] == new_date].index
if len(rows_to_drop):
    df_existing = df_existing.drop(rows_to_drop)
    print(f"-{len(rows_to_drop)} rows", file=sys.stderr)

df3 = pd.concat([df_existing, df_new])
print(f"+{len(df_new)} rows", file=sys.stderr)

df3 = df3.sort_values(by=["date"], kind="stable")
df3 = df3.reset_index(drop=True)

df3.to_feather(filename_existing)
