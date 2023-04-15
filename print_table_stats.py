# pyright: strict

import sys

import polars as pl

from polars_utils import describe_frame

filename = sys.argv[1]

pl.enable_string_cache(True)

if filename.endswith(".arrow"):
    df = pl.read_ipc(filename)
elif filename.endswith(".parquet"):
    df = pl.read_parquet(filename)
else:
    raise ValueError(f"Unknown file extension: {filename}")

describe_frame(df, source=filename, output=sys.stdout)
