# pyright: strict

import os
import sys

import polars as pl

STEP_SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null")

txt_out = sys.stdout
md_out = open(STEP_SUMMARY, "w")

filename = sys.argv[1]

pl.toggle_string_cache(True)

df = pl.read_ipc(filename, memory_map=False)
table = df.to_arrow()  # type: ignore
count = len(df)


def count_columns(column_name: str, expr: pl.Expr) -> pl.DataFrame:
    df2 = df.select(expr)
    if df2.is_empty():
        schema = {"column": pl.Utf8, column_name: pl.UInt32}
        return pl.DataFrame(schema=schema)
    return df2.transpose(include_header=True, column_names=[column_name])


null_count_df = count_columns("null_count", pl.all().null_count())
is_unique_df = count_columns("is_unique", pl.all().drop_nulls().is_unique().all())
true_count_df = count_columns("true_count", pl.col(pl.Boolean).drop_nulls().sum())
false_count_df = count_columns(
    "false_count", pl.col(pl.Boolean).drop_nulls().is_not().sum()
)

schema_df = (
    (
        null_count_df.join(is_unique_df, on="column", how="left")
        .join(true_count_df, on="column", how="left")
        .join(false_count_df, on="column", how="left")
    )
    .with_columns(
        pl.col("column").apply(lambda n: table[n].type).alias("dtype"),  # type: ignore
        pl.col("true_count").fill_null(0),
        pl.col("false_count").fill_null(0),
    )
    .with_columns(
        (pl.col("true_count") / count).alias("true_percent"),
        (pl.col("false_count") / count).alias("false_percent"),
        (pl.col("null_count") / count).alias("null_percent"),
    )
)

for row in schema_df.iter_rows(named=True):
    print(f"{row['column']}: {row['dtype']}", file=txt_out)
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
    print(f"|{row['column']}|{row['dtype']}", file=md_out, end="|")
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

mb = df.estimated_size("mb")
if mb > 2:
    print(f"  rss: {mb:,.1f}MB", file=txt_out)
    print(f"rss: {mb:,.1f}MB", file=md_out)
else:
    kb = df.estimated_size("kb")
    print(f"  rss: {kb:,.1f}KB", file=txt_out)
    print(f"rss: {kb:,.1f}KB", file=md_out)
