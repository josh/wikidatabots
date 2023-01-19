import polars as pl


def reindex_as_range(df: pl.DataFrame, name: str) -> pl.DataFrame:
    col = df[name]
    lower, upper = col.min(), col.max()
    assert isinstance(lower, int) and isinstance(upper, int)
    assert lower >= 0
    values = range(upper + 1)
    index = pl.Series(name=name, values=values, dtype=col.dtype)
    return index.to_frame().join(df, on=name, how="left")
