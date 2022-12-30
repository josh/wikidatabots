# pyright: basic


import os
import sys
import time

import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather

STEP_SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null")

txt_out = sys.stdout
md_out = open(STEP_SUMMARY, "w")

filename = sys.argv[1]

start = time.time()
table = feather.read_table(filename, memory_map=False)
elapsed = time.time() - start
count = len(table)

df = pd.read_feather(filename)

print(f"## {filename}", file=md_out)
print("|name|pyarrow|pandas|", file=md_out)
print("|---|---|---|", file=md_out)

for column_name in table.column_names:
    col = table[column_name]
    ary = col.combine_chunks()

    if df.index.name == column_name:
        pd_dtype = df.index.dtype
    else:
        pd_dtype = df[column_name].dtype

    print(f"{column_name}: {col.type}[pyarrow] / {pd_dtype}[pandas]", file=txt_out)
    print(f"|{column_name}|{col.type}|{pd_dtype}|", file=md_out)
    if col.type == pa.bool_():
        print(
            f"|   true: {ary.true_count:,} ({ary.true_count/count:.2%})",
            file=txt_out,
        )
        print(
            f"|  false: {ary.false_count:,} ({ary.false_count/count:.2%})",
            file=txt_out,
        )

    if ary.null_count:
        print(
            f"|   null: {ary.null_count:,} ({ary.null_count/count:.2%})",
            file=txt_out,
        )

print(f"total: {count:,}", file=txt_out)
print(f" load: {elapsed:0.2}s", file=txt_out)

print("", file=md_out)
print(f"total: {count:,} rows", file=md_out)
print(f"load: {elapsed:0.2}s", file=md_out)

kb = pa.total_allocated_bytes() >> 10
mb = pa.total_allocated_bytes() >> 20
if mb > 2:
    print(f"  rss: {mb:,}MB", file=txt_out)
    print(f"rss: {mb:,}MB", file=md_out)
else:
    print(f"  rss: {kb:,}KB", file=txt_out)
    print(f"rss: {kb:,}KB", file=md_out)
