import os
import sys
import warnings

import polars as pl

import actions
from polars_utils import row_differences, unique_row_differences

actions.install_warnings_hook()

STEP_SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null")

txt_out = sys.stdout
md_out = open(STEP_SUMMARY, "w")

with pl.StringCache():
    try:
        df_a = pl.read_ipc(sys.argv[1], memory_map=False)
        df_b = pl.read_ipc(sys.argv[2], memory_map=False)
    except:  # noqa: E722
        warnings.warn("Falling back to pyarrow")
        df_a = pl.read_ipc(sys.argv[1], use_pyarrow=True, memory_map=False)
        df_b = pl.read_ipc(sys.argv[2], use_pyarrow=True, memory_map=False)

if len(sys.argv) > 3:
    key = sys.argv[3]
    added, removed, updated = unique_row_differences(df_a, df_b, on=key)
    print(f"+{added:,} -{removed:,} ~{updated:,}", file=txt_out)

    print(f"## {sys.argv[1]} vs {sys.argv[2]}", file=md_out)
    print(f"+{added:,} -{removed:,} ~{updated:,}", file=md_out)

else:
    added, removed = row_differences(df_a, df_b)
    print(f"+{added:,} -{removed:,}", file=txt_out)

    print(f"## {sys.argv[1]} vs {sys.argv[2]}", file=md_out)
    print(f"+{added:,} -{removed:,}", file=md_out)
