# pyright: strict

import sys

import polars as pl

from polars_utils import frame_diff

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
changes = frame_diff(df_a, df_b, on=key).collect().row(0, named=True)
added, removed, updated = changes["added"], changes["removed"], changes["updated"]

print(f"## {sys.argv[1]} vs {sys.argv[2]}")
print(f"+{added:,} -{removed:,} ~{updated:,}")
