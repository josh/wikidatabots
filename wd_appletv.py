# pyright: strict

import polars as pl

from appletv_etl import LOC_SHOW_PATTERN, url_extract_id, valid_appletv_id
from polars_utils import print_rdf_statements, sample
from sparql import sparql, sparql_batch
from wikidata import is_blocked_item

_SEARCH_LIMIT = 500

_SEARCH_QUERY = """
SELECT DISTINCT ?item (SAMPLE(?appletv_id) AS ?has_appletv) WHERE {
  VALUES ?title { {} }
  VALUES ?directorName { {} }
  VALUES ?year { {} {} }

  SERVICE wikibase:mwapi {
    bd:serviceParam wikibase:endpoint "www.wikidata.org";
                    wikibase:api "EntitySearch";
                    mwapi:search {};
                    mwapi:language "en".
    ?item wikibase:apiOutputItem mwapi:item.
  }

  VALUES ?classes {
    wd:Q11424 # film
    wd:Q506240 # television film
  }
  ?item (wdt:P31/(wdt:P279*)) ?classes.

  OPTIONAL { ?item rdfs:label ?titleLabel. }
  OPTIONAL { ?item skos:altLabel ?titleAltLabel. }
  FILTER(((LCASE(STR(?titleLabel))) = LCASE(?title)) ||
         ((LCASE(STR(?titleAltLabel))) = LCASE(?title)))

  ?item wdt:P577 ?date.
  FILTER((xsd:integer(YEAR(?date))) = ?year)

  ?item wdt:P57 ?director.
  ?director rdfs:label ?directorLabel.
  FILTER((STR(?directorLabel)) = ?directorName)

  OPTIONAL { ?item wdt:P9586 ?appletv_id. }
}
GROUP BY ?item
"""


def _escape_str(expr: pl.Expr) -> pl.Expr:
    return expr.str.replace_all('"', '\\"', literal=True)


def _quote_str(expr: pl.Expr) -> pl.Expr:
    return pl.format('"{}"', _escape_str(expr))


def _quote_arr_str(expr: pl.Expr) -> pl.Expr:
    return pl.format(
        '"{}"',
        expr.arr.eval(_escape_str(pl.element())).arr.join('" "'),
    )


def find_wd_movie_via_search(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        pl.format(
            _SEARCH_QUERY,
            pl.col("title").pipe(_quote_str),
            pl.col("directors").pipe(_quote_arr_str),
            pl.col("published_at").dt.year(),
            pl.col("published_at").dt.year() + 1,
            pl.col("title").pipe(_quote_str),
        )
        .pipe(sparql_batch, columns=["item", "has_appletv"])
        .alias("results"),
    )


_ANY_ID_QUERY = """
SELECT DISTINCT ?id WHERE { ?statement ps:P9586 ?id. }
"""

_ADD_RDF_STATEMENT = pl.format(
    '<{}> wdt:P9586 "{}" .', pl.col("item"), pl.col("id")
).alias("rdf_statement")


def _find_movie_via_search(sitemap_df: pl.LazyFrame) -> pl.LazyFrame:
    wd_df = (
        sparql(_ANY_ID_QUERY, columns=["id"])
        .select(pl.col("id").pipe(valid_appletv_id))
        .drop_nulls()
        .with_columns(pl.lit(True).alias("wd_exists"))
    )

    return (
        sitemap_df.filter(
            pl.col("country").eq("us")
            & pl.col("in_latest_sitemap")
            & pl.col("jsonld_success")
            & pl.col("title").is_not_null()
            & pl.col("published_at").is_not_null()
            & pl.col("directors").is_not_null()
            & pl.col("directors").arr.lengths().ge(1)
        )
        .join(wd_df, on="id", how="left")
        .filter(pl.col("wd_exists").is_null())
        .sort(pl.col("published_at"), descending=True)
        .head(_SEARCH_LIMIT * 10)
        .pipe(sample, n=_SEARCH_LIMIT)
        .pipe(find_wd_movie_via_search)
        .filter(pl.col("results").arr.lengths() == 1)
        .with_columns(pl.col("results").arr.first().alias("result"))
        .unnest("result")
        .filter(pl.col("item").is_not_null() & pl.col("has_appletv").is_null())
        .select(_ADD_RDF_STATEMENT)
    )


_ITUNES_QUERY = """
SELECT ?item ?itunes_id WHERE {
  ?item wdt:P6398 ?itunes_id.
  FILTER(xsd:integer(?itunes_id))

  # Apple TV movie ID subject type constraints
  VALUES ?class {
    wd:Q11424 # film
  }
  ?item (wdt:P31/(wdt:P279*)) ?class.

  OPTIONAL { ?item wdt:P9586 ?appletv_id. }
  FILTER(!(BOUND(?appletv_id)))
}
"""

_ADD_VIA_ITUNES_STATEMENT = pl.format(
    '<{}> wdt:P9586 "{}"; wikidatabots:editSummary '
    '"Add Apple TV movie ID via associated iTunes movie ID " .',
    pl.col("item"),
    pl.col("appletv_id"),
).alias("rdf_statement")


def _find_movie_via_itunes_redirect(itunes_df: pl.LazyFrame) -> pl.LazyFrame:
    wd_df = sparql(_ITUNES_QUERY, schema={"item": pl.Utf8, "itunes_id": pl.UInt64})

    itunes_df = (
        itunes_df.filter((pl.col("kind") == "feature-movie") & pl.col("any_country"))
        .with_columns(
            pl.col("redirect_url").pipe(url_extract_id).alias("appletv_id"),
        )
        .filter(pl.col("appletv_id").is_not_null())
        .select(pl.col("id").alias("itunes_id"), pl.col("appletv_id"))
    )

    return (
        wd_df.join(itunes_df, on="itunes_id", how="left")
        .filter(
            pl.col("appletv_id").is_not_null()
            & pl.col("item").pipe(is_blocked_item).is_not()
        )
        .select(_ADD_VIA_ITUNES_STATEMENT)
    )


_ITUNES_SEASON_QUERY = """
SELECT ?show_item ?season_item ?itunes_season_id WHERE {
  ?season_item wdt:P6381 ?itunes_season_id;
    wdt:P179 ?show_item.
  OPTIONAL { ?show_item wdt:P9751 ?appletv_id. }
  FILTER(!(BOUND(?appletv_id)))
}
"""

_ITUNES_SEASON_QUERY_SCHEMA: dict[str, pl.PolarsDataType] = {
    "show_item": pl.Utf8,
    "season_item": pl.Utf8,
    "itunes_season_id": pl.UInt64,
}

_ADD_VIA_ITUNES_SEASON_STATEMENT = pl.format(
    '<{}> wdt:P9751 "{}"; wikidatabots:editSummary '
    '"Add Apple TV show ID via associated iTunes TV season ID " .',
    pl.col("show_item"),
    pl.col("appletv_show_id"),
).alias("rdf_statement")


def _find_show_via_itunes_season(itunes_df: pl.LazyFrame) -> pl.LazyFrame:
    wd_df = sparql(
        _ITUNES_SEASON_QUERY,
        schema=_ITUNES_SEASON_QUERY_SCHEMA,
    )

    itunes_seasons_df = (
        itunes_df.filter(pl.col("type") == "TV Season")
        .filter(pl.col("any_country"))
        .select(
            pl.col("id").alias("itunes_season_id"),
            pl.col("redirect_url")
            .str.extract(LOC_SHOW_PATTERN, 1)
            .alias("appletv_show_id"),
        )
        .filter(pl.col("appletv_show_id").is_not_null())
        .select("itunes_season_id", "appletv_show_id")
    )

    return (
        wd_df.join(itunes_seasons_df, on="itunes_season_id", how="left")
        .filter(
            pl.col("appletv_show_id").is_not_null()
            & pl.col("show_item").pipe(is_blocked_item).is_not()
        )
        .select("show_item", "appletv_show_id")
        .unique()
        .select(_ADD_VIA_ITUNES_SEASON_STATEMENT)
    )


def main() -> None:
    sitemap_df = pl.scan_parquet(
        "s3://wikidatabots/appletv/movie.parquet",
        storage_options={"anon": True},
    )
    itunes_df = pl.scan_parquet(
        "s3://wikidatabots/itunes.parquet",
        storage_options={"anon": True},
    )

    pl.concat(
        [
            _find_movie_via_search(sitemap_df),
            _find_show_via_itunes_season(itunes_df),
            _find_movie_via_itunes_redirect(itunes_df),
        ]
    ).pipe(print_rdf_statements, sample=False)


if __name__ == "__main__":
    main()
