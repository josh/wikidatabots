# pyright: basic

import sys
import time

import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather

filename = sys.argv[1]

start = time.time()
table = feather.read_table(filename, memory_map=False)
elapsed = time.time() - start
count = len(table)

df = pd.read_feather(filename)

for column_name in table.column_names:
    col = table[column_name]
    ary = col.combine_chunks()

    if df.index.name == column_name:
        pd_dtype = df.index.dtype
    else:
        pd_dtype = df[column_name].dtype

    print(f"{column_name}: {col.type}[pyarrow] / {pd_dtype}[pandas]")
    if col.type == pa.bool_():
        print(f"|   true: {ary.true_count:,} ({ary.true_count/count:.2%})")
        print(f"|  false: {ary.false_count:,} ({ary.false_count/count:.2%})")

    if ary.null_count:
        print(f"|   null: {ary.null_count:,} ({ary.null_count/count:.2%})")

print(f"total: {count:,}")
print(f" load: {elapsed:0.2}s")

kb = pa.total_allocated_bytes() >> 10
mb = pa.total_allocated_bytes() >> 20
if mb > 2:
    print(f"  rss: {mb:,}MB")
else:
    print(f"  rss: {kb:,}KB")

# if table.schema.metadata:
#     print("-- schema metadata --")
#     sys.stdout.flush()
#     for key, value in table.schema.metadata.items():
#         sys.stdout.buffer.write(key)
#         sys.stdout.write(": ")
#         try:
#             json.dump(json.loads(value), sys.stdout, indent=2)
#             sys.stdout.write("\n")
#         except json.JSONDecodeError:
#             sys.stdout.buffer.write(value)
