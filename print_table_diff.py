# pyright: basic

import sys

import pandas as pd

from pandas_utils import df_diff

key = sys.argv[1]
df_a = pd.read_feather(sys.argv[2])
df_b = pd.read_feather(sys.argv[3])

(added, removed, updated) = df_diff(df_a, df_b, key=key)
print(f"+{added:,} -{removed:,} ~{updated:,}")
