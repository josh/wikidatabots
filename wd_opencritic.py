# pyright: strict

import polars as pl

from polars_utils import print_rdf_statements
from sparql import sparql
from wikidata import is_blocked_item

_NUM_REVIEWS_THRESHOLD = 10

_QUERY = """
SELECT ?item ?opencritic_id ?statement
      ?review_score ?point_in_time ?number_of_reviews WHERE {
  ?item wdt:P2864 ?opencritic_id.
  FILTER(xsd:integer(?opencritic_id))

  OPTIONAL {
    ?item p:P444 ?statement.

    ?statement wikibase:rank ?rank.
    FILTER(?rank != wikibase:DeprecatedRank)

    ?statement ps:P444 ?review_score.
    ?statement pq:P447 wd:Q21039459.

    ?statement pq:P459 ?determination_method.
    OPTIONAL { ?statement pq:P585 ?point_in_time. }
    OPTIONAL { ?statement pq:P7887 ?number_of_reviews. }
  }
}
"""

_QUERY_SCHEMA: dict[str, pl.PolarsDataType] = {
    "item": pl.Utf8,
    "opencritic_id": pl.UInt32,
    "statement": pl.Utf8,
    "review_score": pl.Utf8,
    "point_in_time": pl.Utf8,
    "number_of_reviews": pl.Float64,
}

_ADD_STATEMENT_TEMPLATE = """
<{}> p:P444 [
  ps:P444 "{}";
  pqe:P447 wd:Q21039459;
  pqe:P459 wd:{};
  pqe:P585 "{}"^^xsd:date;
  pqve:P7887 [
    rdf:type wikibase:QuantityValue;
    wikibase:quantityAmount "{}"^^xsd:decimal;
    wikibase:quantityUnit wd:Q80698083
  ];
  prov:wasOnlyDerivedFrom [
    pr:P248 wd:Q21039459;
    pr:P2864 "{}";
    pr:P813 "{}"^^xsd:date
  ];
  wikidatabots:editSummary "Add OpenCritic review score"
].
"""

_UPDATE_STATEMENT_TEMPLATE = """
<{}>
  ps:P444 "{}";
  pqe:P447 wd:Q21039459;
  pqe:P459 wd:{};
  pqe:P585 "{}"^^xsd:date;
  pqve:P7887 [
    rdf:type wikibase:QuantityValue;
    wikibase:quantityAmount "{}"^^xsd:decimal;
    wikibase:quantityUnit wd:Q80698083
  ];
  prov:wasOnlyDerivedFrom [
    pr:P248 wd:Q21039459;
    pr:P2864 "{}";
    pr:P813 "{}"^^xsd:date
  ];
  wikidatabots:editSummary "Update OpenCritic review score".
"""


def _wd_review_scores(determination_method_qid: str) -> pl.LazyFrame:
    query = _QUERY.replace("?determination_method", f"wd:{determination_method_qid}")
    return (
        sparql(query, schema=_QUERY_SCHEMA)
        .unique("item", keep="none")
        .with_columns(
            pl.col("item")
            .str.replace("http://www.wikidata.org/entity/", "")
            .alias("qid")
        )
        .with_columns(
            pl.col("number_of_reviews").cast(pl.UInt16),
            pl.col("point_in_time").str.strptime(pl.Date, "%+"),
        )
        .select(pl.all().prefix("wd_"))
    )


def _rdf_statement(determination_method_qid: str) -> pl.Expr:
    return (
        pl.when(pl.col("wd_statement").is_null())
        .then(
            pl.format(
                _ADD_STATEMENT_TEMPLATE,
                "wd_item",
                "api_review_score",
                pl.lit(determination_method_qid),
                "api_latest_review_date",
                "api_num_reviews",
                "wd_opencritic_id",
                "api_retrieved_on",
            )
        )
        .otherwise(
            pl.format(
                _UPDATE_STATEMENT_TEMPLATE,
                "wd_statement",
                "api_review_score",
                pl.lit(determination_method_qid),
                "api_latest_review_date",
                "api_num_reviews",
                "wd_opencritic_id",
                "api_retrieved_on",
            )
        )
        .alias("rdf_statement")
    )


def _find_opencritic_top_critic_score() -> pl.LazyFrame:
    determination_method_qid = "Q114712322"

    wd_df = _wd_review_scores(determination_method_qid)

    api_df = pl.scan_parquet(
        "s3://wikidatabots/opencritic.parquet",
        storage_options={"anon": True},
    ).select(pl.all().prefix("api_"))

    return (
        wd_df.join(api_df, left_on="wd_opencritic_id", right_on="api_id", how="left")
        .filter(
            pl.col("wd_qid").pipe(is_blocked_item).is_not()
            & pl.col("api_top_critic_score").is_not_null()
            & pl.col("api_latest_review_date").is_not_null()
            & pl.col("api_retrieved_at").is_not_null()
            & pl.col("api_num_reviews").gt(0)
        )
        .with_columns(
            pl.format(
                "{}/100", pl.col("api_top_critic_score").round(0).cast(pl.UInt8)
            ).alias("api_review_score"),
            pl.col("api_retrieved_at").dt.date().alias("api_retrieved_on"),
        )
        .filter(
            pl.col("wd_review_score").is_null()
            | pl.col("wd_number_of_reviews").is_null()
            | pl.col("wd_review_score").ne(pl.col("api_review_score"))
            | pl.col("wd_number_of_reviews")
            .add(_NUM_REVIEWS_THRESHOLD)
            .lt(pl.col("api_num_reviews"))
        )
        .select(_rdf_statement(determination_method_qid))
    )


def _find_opencritic_percent_recommended() -> pl.LazyFrame:
    determination_method_qid = "Q119576498"

    wd_df = _wd_review_scores(determination_method_qid)

    api_df = pl.scan_parquet(
        "s3://wikidatabots/opencritic.parquet",
        storage_options={"anon": True},
    ).select(pl.all().prefix("api_"))

    return (
        wd_df.join(api_df, left_on="wd_opencritic_id", right_on="api_id", how="left")
        .filter(
            pl.col("wd_qid").pipe(is_blocked_item).is_not()
            & pl.col("api_percent_recommended").is_not_null()
            & pl.col("api_latest_review_date").is_not_null()
            & pl.col("api_retrieved_at").is_not_null()
            & pl.col("api_percent_recommended").gt(0)
            & pl.col("api_num_reviews").gt(0)
        )
        .with_columns(
            pl.format(
                "{}%", pl.col("api_percent_recommended").round(0).cast(pl.UInt8)
            ).alias("api_review_score"),
            pl.col("api_retrieved_at").dt.date().alias("api_retrieved_on"),
        )
        .filter(
            pl.col("wd_review_score").is_null()
            | pl.col("wd_number_of_reviews").is_null()
            | pl.col("wd_review_score").ne(pl.col("api_review_score"))
            | pl.col("wd_number_of_reviews")
            .add(_NUM_REVIEWS_THRESHOLD)
            .lt(pl.col("api_num_reviews"))
        )
        .select(_rdf_statement(determination_method_qid))
    )


def _main() -> None:
    pl.enable_string_cache(True)

    _find_opencritic_top_critic_score().pipe(print_rdf_statements)

    # Slowly rollout
    _find_opencritic_percent_recommended().pipe(print_rdf_statements, limit=50)

    # pl.concat(
    #     [
    #         _find_opencritic_top_critic_score(),
    #         _find_opencritic_percent_recommended(),
    #     ]
    # ).pipe(print_rdf_statements)


if __name__ == "__main__":
    _main()
