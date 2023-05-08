# pyright: strict

import logging
import platform
import time
from functools import partial

import backoff
import polars as pl
import requests as _requests

from actions import warn
from polars_utils import apply_with_tqdm, csv_extract

_USER_AGENT_STR = f"Josh404Bot/1.0 (User:Josh404Bot) Python/{platform.python_version()}"


class SlowQueryWarning(Warning):
    pass


@backoff.on_exception(
    backoff.expo,
    _requests.exceptions.RequestException,
    max_tries=10,
)
def _sparql(query: str, _log_query: bool) -> bytes:
    if _log_query:
        logging.info(query)

    start = time.time()
    r = _requests.post(
        "https://query.wikidata.org/sparql",
        data={"query": query},
        headers={"Accept": "text/csv", "User-Agent": _USER_AGENT_STR},
        timeout=(1, 90),
    )
    r.raise_for_status()
    duration = time.time() - start

    if duration > 45:
        logging.warn(f"sparql: {duration:,.2f}s")
        warn(f"sparql: {duration:,.2f}s", SlowQueryWarning)
    elif duration > 5:
        logging.info(f"sparql: {duration:,.2f}s")
    else:
        logging.debug(f"sparql: {duration:,.2f}s")

    return r.content


def _sparql_batch_raw(queries: pl.Series) -> pl.Series:
    return (
        queries.to_frame("query")
        .select(
            pl.col("query").pipe(
                apply_with_tqdm,
                partial(_sparql, _log_query=len(queries) == 1),
                return_dtype=pl.Binary,
                log_group="sparql",
            )
        )
        .to_series()
    )


def sparql(
    query: str,
    columns: list[str] | None = None,
    schema: dict[str, pl.PolarsDataType] | None = None,
) -> pl.LazyFrame:
    if columns and not schema:
        schema = {column: pl.Utf8 for column in columns}
    assert schema, "missing schema"

    def read_item_as_csv(df: pl.DataFrame) -> pl.DataFrame:
        return pl.read_csv(df.item(), dtypes=schema)

    return (
        pl.LazyFrame({"query": [query]})
        .select(
            pl.col("query")
            # MARK: pl.Expr.map
            .map(_sparql_batch_raw, return_dtype=pl.Binary).alias("results"),
        )
        .map(read_item_as_csv, schema=schema)
    )


def sparql_batch(
    queries: pl.Expr,
    columns: list[str] | None = None,
    schema: dict[str, pl.PolarsDataType] | None = None,
) -> pl.Expr:
    if columns and not schema:
        schema = {column: pl.Utf8 for column in columns}
    assert schema, "missing schema"

    dtype = pl.List(pl.Struct(schema))

    # MARK: pl.Expr.map
    return queries.map(_sparql_batch_raw, return_dtype=pl.Binary).pipe(
        csv_extract, dtype=dtype, log_group="parse_sparql_csv"
    )
