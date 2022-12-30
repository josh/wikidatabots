# pyright: basic

import os
import sys

import pandas as pd

from pandas_utils import df_diff

STEP_SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null")

txt_out = sys.stdout
md_out = open(STEP_SUMMARY, "w")

df_a = pd.read_feather(sys.argv[1])
df_b = pd.read_feather(sys.argv[2])
key = None
if len(sys.argv) > 3:
    key = sys.argv[3]

(added, removed, updated) = df_diff(df_a, df_b, on=key)
print(f"+{added:,} -{removed:,} ~{updated:,}", file=txt_out)

print(f"## {sys.argv[1]} vs {sys.argv[2]}", file=md_out)
print(f"+{added:,} -{removed:,} ~{updated:,}", file=md_out)
