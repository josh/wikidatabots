# pyright: strict

import os
import sys

import polars as pl

from polars_utils import unique_row_differences

STEP_SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null")

txt_out = sys.stdout
md_out = open(STEP_SUMMARY, "w")

pl.toggle_string_cache(True)


def read_df(filename: str) -> pl.LazyFrame:
    if filename.endswith(".arrow") or filename.endswith(".arrow~"):
        return pl.scan_ipc(filename, memory_map=False)
    elif filename.endswith(".parquet") or filename.endswith(".parquet~"):
        return pl.scan_parquet(filename)
    else:
        raise ValueError(f"Unknown file extension: {filename}")


df_a = read_df(sys.argv[1])
df_b = read_df(sys.argv[2])

key = sys.argv[3]
added, removed, updated = unique_row_differences(df_a, df_b, on=key)
print(f"+{added:,} -{removed:,} ~{updated:,}", file=txt_out)

print(f"## {sys.argv[1]} vs {sys.argv[2]}", file=md_out)
print(f"+{added:,} -{removed:,} ~{updated:,}", file=md_out)
