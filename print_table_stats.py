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
with pl.StringCache():
    df = pl.read_ipc(filename, memory_map=False)
count = len(df)


def count_columns(column_name: str, expr: pl.Expr) -> pl.DataFrame:
    return df.select(expr).transpose(include_header=True, column_names=[column_name])


null_count_df = count_columns("null_count", pl.col("*").null_count())
n_unique_df = count_columns("n_unique", pl.col("*").n_unique())
true_count_df = count_columns("true_count", pl.col(pl.Boolean).sum())
false_count_df = count_columns("false_count", pl.col(pl.Boolean).is_not().sum())

schema_df = (
    (
        null_count_df.join(n_unique_df, on="column")
        .join(true_count_df, on="column", how="left")
        .join(false_count_df, on="column", how="left")
        .rename({"column": "name"})
    )
    .with_columns(
        pl.col("name").apply(lambda n: table[n].type).alias("dtype"),
        pl.col("true_count").fill_null(0),
        pl.col("false_count").fill_null(0),
    )
    .with_columns(
        (pl.col("n_unique") == count).alias("is_unique"),
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
