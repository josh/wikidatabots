import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather

STEP_SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY", "/dev/null")

txt_out = sys.stdout
md_out = open(STEP_SUMMARY, "w")

filename = sys.argv[1]

table = feather.read_table(filename)
df = pd.read_feather(filename)
count = len(df)

schema_df = pd.DataFrame({"name": table.column_names}, index=table.column_names)
schema_df["pyarrow"] = schema_df.name.apply(lambda n: table[n].type)
schema_df["pandas"] = df.dtypes

bool_colnames = [n for n in table.column_names if table[n].type == pa.bool_()]
bool_cols = df[bool_colnames]
if bool_cols.empty:
    schema_df["true_count"] = 0
    schema_df["false_count"] = 0
else:
    schema_df["true_count"] = bool_cols.where(df == 1).notna().sum()
    schema_df["false_count"] = bool_cols.where(df == 0).notna().sum()

schema_df["null_count"] = df.isna().sum()
schema_df["true_percent"] = schema_df.true_count / count
schema_df["false_percent"] = schema_df.false_count / count
schema_df["null_percent"] = schema_df.null_count / count
schema_df["is_unique"] = df.nunique(dropna=True) == (count - schema_df.null_count)

for row in schema_df.itertuples():
    print(f"{row.Index}: {row.pyarrow}[pyarrow] / {row.pandas}[pandas]", file=txt_out)
    if row.true_count > 0 or row.false_count > 0:
        print(
            f"|   true: {row.true_count:,} ({row.true_percent:.2%})",
            file=txt_out,
        )
        print(
            f"|   false: {row.false_count:,} ({row.false_percent:.2%})",
            file=txt_out,
        )

    if row.null_count > 0:
        print(
            f"|   null: {row.null_count:,} ({row.null_percent:.2%})",
            file=txt_out,
        )

    if row.is_unique:
        print("|   unique", file=txt_out)

print(f"## {filename}", file=md_out)
print("|name|pyarrow|pandas|null|true|false|unique|", file=md_out)
print("|---|---|---|---|---|---|---|", file=md_out)
for row in schema_df.itertuples():
    print(f"|{row.name}|{row.pyarrow}|{row.pandas}", file=md_out, end="|")
    if row.null_count > 0:
        print(f"{row.null_count:,} ({row.null_percent:.2%})", file=md_out, end="|")
    else:
        print("", file=md_out, end="|")
    if row.true_count > 0 or row.false_count > 0:
        print(f"{row.true_count:,} ({row.true_percent:.2%})", file=md_out, end="|")
        print(f"{row.false_count:,} ({row.false_percent:.2%})", file=md_out, end="|")
    else:
        print("||", file=md_out, end="")
    if row.is_unique:
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
