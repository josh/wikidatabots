import polars as pl


def reindex_as_range(df: pl.DataFrame, name: str) -> pl.DataFrame:
    col = df[name]
    upper = col.max()
    assert isinstance(upper, int)
    values = range(upper + 1)
    index = pl.Series(name=name, values=values, dtype=col.dtype)
    return df.join(index.to_frame(), on=name, how="outer")
