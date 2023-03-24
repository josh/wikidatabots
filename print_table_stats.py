# pyright: strict

import sys

import polars as pl

filename = sys.argv[1]

pl.toggle_string_cache(True)

if filename.endswith(".arrow"):
    df = pl.read_ipc(filename, memory_map=False)
elif filename.endswith(".parquet"):
    df = pl.read_parquet(filename)
else:
    raise ValueError(f"Unknown file extension: {filename}")

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


def _comma(value: int) -> str:
    return f"{value:,}"


def _percent(value: float) -> str:
    return f"{value:.2%}"


summary_df = (
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
    .select(
        pl.col("column").alias("name"),
        pl.col("dtype").alias("dtype"),
        pl.when(pl.col("null_count") > 0)
        .then(
            pl.format(
                "{} ({})",
                pl.col("null_count").apply(_comma, return_dtype=pl.Utf8),
                (pl.col("null_count") / count).apply(_percent, return_dtype=pl.Utf8),
            )
        )
        .otherwise("")
        .alias("null"),
        pl.when(pl.col("true_count") > 0)
        .then(
            pl.format(
                "{} ({})",
                pl.col("true_count").apply(_comma, return_dtype=pl.Utf8),
                (pl.col("true_count") / count).apply(_percent, return_dtype=pl.Utf8),
            )
        )
        .otherwise("")
        .alias("true"),
        pl.when(pl.col("false_count") > 0)
        .then(
            pl.format(
                "{} ({})",
                pl.col("false_count").apply(_comma, return_dtype=pl.Utf8),
                (pl.col("false_count") / count).apply(_percent, return_dtype=pl.Utf8),
            )
        )
        .otherwise("")
        .alias("false"),
        pl.when(pl.col("is_unique")).then("true").otherwise("").alias("unique"),
    )
)

pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_column_data_type_inline(True)
pl.Config.set_tbl_formatting("ASCII_MARKDOWN")
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_width_chars(500)

print(f"## {filename}")
print(summary_df)
print(f"\nshape: {df.shape}")

mb = df.estimated_size("mb")
if mb > 2:
    print(f"rss: {mb:,.1f}MB")
else:
    kb = df.estimated_size("kb")
    print(f"rss: {kb:,.1f}KB")
