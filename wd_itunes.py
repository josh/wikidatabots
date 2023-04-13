# pyright: strict

import polars as pl

from constants import ITUNES_MOVIE_ID_PID
from itunes import COUNTRIES
from itunes_etl import check_itunes_id
from sparql import fetch_statements_df, sample_items

_EDIT_SUMMARY = "Deprecate iTunes movie ID delisted from store"


def _deprecated_rdf_statement() -> pl.Expr:
    return pl.format(
        "<{}> wikibase:rank wikibase:DeprecatedRank ; "
        "pq:P2241 wd:Q21441764 ; "
        f'wikidatabots:editSummary "{_EDIT_SUMMARY}" . ',
        pl.col("statement"),
    ).alias("rdf_statement")


def _delisted_itunes_ids() -> pl.LazyFrame:
    # TODO: Fetch lazily
    qids = sample_items(ITUNES_MOVIE_ID_PID, limit=5_000)

    return (
        fetch_statements_df(qids, [ITUNES_MOVIE_ID_PID])
        .with_columns(
            pl.col("value")
            .str.parse_int(10, strict=False)
            .cast(pl.UInt64)
            .alias("itunes_id")
        )
        .filter(pl.col("itunes_id").is_not_null())
        .with_columns(
            pl.col("itunes_id").pipe(check_itunes_id, country="us").alias("country_us"),
        )
        .filter(pl.col("country_us").is_not())
        .with_columns(
            pl.Expr.or_(
                *[
                    pl.col("itunes_id").pipe(check_itunes_id, country=country)
                    for country in COUNTRIES
                ]
            ).alias("country_any"),
        )
        .filter(pl.col("country_any").is_not())
        .select(_deprecated_rdf_statement())
    )


def main() -> None:
    df = _delisted_itunes_ids()

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
