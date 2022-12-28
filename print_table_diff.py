# pyright: basic

import sys

import pandas as pd

from pandas_utils import df_diff

df_a = pd.read_feather(sys.argv[1])
df_b = pd.read_feather(sys.argv[2])
key = None
if len(sys.argv) > 3:
    key = sys.argv[3]

(added, removed, updated) = df_diff(df_a, df_b, on=key)
print(f"+{added:,} -{removed:,} ~{updated:,}")
