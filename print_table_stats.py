import os
import sys

import polars as pl
import pyarrow as pa
import pyarrow.feather as feather

STEP_SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null")

txt_out = sys.stdout
md_out = open(STEP_SUMMARY, "w")

filename = sys.argv[1]

table = feather.read_table(filename)
df = pl.read_ipc(filename, memory_map=False)
count = len(df)


def true_count(colname: str) -> int:
    if table[colname].type == pa.bool_():
        return len(df.filter(pl.col(colname)))
    else:
        return 0


def false_count(colname: str) -> int:
    if table[colname].type == pa.bool_():
        return len(df.filter(pl.col(colname).is_not()))
    else:
        return 0


def null_count(colname: str) -> int:
    return len(df.filter(pl.col(colname).is_null()))


def is_unique(colname: str) -> bool:
    return df.n_unique(subset=colname) == count


schema_df = (
    pl.DataFrame({"name": table.column_names})
    .with_columns(
        pl.col("name").apply(lambda n: table[n].type).alias("dtype"),
        pl.col("name").apply(lambda n: table[n].type == pa.bool_()).alias("is_bool"),
    )
    .with_columns(
        pl.col("name").apply(true_count).alias("true_count"),
        pl.col("name").apply(false_count).alias("false_count"),
        pl.col("name").apply(null_count).alias("null_count"),
        pl.col("name").apply(is_unique).alias("is_unique"),
    )
    .with_columns(
        (pl.col("true_count") / count).alias("true_percent"),
        (pl.col("false_count") / count).alias("false_percent"),
        (pl.col("null_count") / count).alias("null_percent"),
    )
)

for row in schema_df.iter_rows(named=True):
    print(f"{row['name']}: {row['dtype']}", file=txt_out)
    if row["true_count"] > 0 or row["false_count"] > 0:
        print(
            f"|   true: {row['true_count']:,} ({row['true_percent']:.2%})",
            file=txt_out,
        )
        print(
            f"|   false: {row['false_count']:,} ({row['false_percent']:.2%})",
            file=txt_out,
        )

    if row["null_count"] > 0:
        print(
            f"|   null: {row['null_count']:,} ({row['null_percent']:.2%})",
            file=txt_out,
        )

    if row["is_unique"]:
        print("|   unique", file=txt_out)

print(f"## {filename}", file=md_out)
print("|name|dtype|null|true|false|unique|", file=md_out)
print("|---|---|---|---|---|---|", file=md_out)
for row in schema_df.iter_rows(named=True):
    print(f"|{row['name']}|{row['dtype']}", file=md_out, end="|")
    if row["null_count"] > 0:
        print(
            f"{row['null_count']:,} ({row['null_percent']:.2%})", file=md_out, end="|"
        )
    else:
        print("", file=md_out, end="|")
    if row["true_count"] > 0 or row["false_count"] > 0:
        print(
            f"{row['true_count']:,} ({row['true_percent']:.2%})", file=md_out, end="|"
        )
        print(
            f"{row['false_count']:,} ({row['false_percent']:.2%})", file=md_out, end="|"
        )
    else:
        print("||", file=md_out, end="")
    if row["is_unique"]:
        print("true", file=md_out, end="|")
    else:
        print("", file=md_out, end="|")
    print("", file=md_out)

print(f"total: {count:,}", file=txt_out)

print("", file=md_out)
print(f"total: {count:,} rows", file=md_out)

kb = pa.total_allocated_bytes() >> 10
mb = pa.total_allocated_bytes() >> 20
if mb > 2:
    print(f"  rss: {mb:,}MB", file=txt_out)
    print(f"rss: {mb:,}MB", file=md_out)
else:
    print(f"  rss: {kb:,}KB", file=txt_out)
    print(f"rss: {kb:,}KB", file=md_out)
