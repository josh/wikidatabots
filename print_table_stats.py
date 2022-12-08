# pyright: basic

import sys
import time

import pyarrow as pa
import pyarrow.feather as feather

filename = sys.argv[1]

start = time.time()
table = feather.read_table(filename, memory_map=False)
elapsed = time.time() - start
count = len(table)

for column_name in table.column_names:
    col = table[column_name]
    ary = col.combine_chunks()

    print(f"{column_name}: {col.type}")
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
