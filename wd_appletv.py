import polars as pl

from appletv_etl import valid_appletv_id
from polars_utils import print_rdf_statements, scan_s3_parquet_anon, weighted_sample
from sparql import sparql, sparql_batch

_SEARCH_LIMIT = 1_000

_SEARCH_MOVIE_QUERY = """
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

_SEARCH_SHOW_QUERY = """
SELECT DISTINCT ?item (SAMPLE(?appletv_id) AS ?has_appletv) WHERE {
  VALUES ?title { {} }
  VALUES ?year { {} {} }

  SERVICE wikibase:mwapi {
    bd:serviceParam wikibase:endpoint "www.wikidata.org";
                    wikibase:api "EntitySearch";
                    mwapi:search {};
                    mwapi:language "en".
    ?item wikibase:apiOutputItem mwapi:item.
  }

  VALUES ?classes {
    wd:Q5398426 # television series
  }
  ?item (wdt:P31/(wdt:P279*)) ?classes.

  OPTIONAL { ?item rdfs:label ?titleLabel. }
  OPTIONAL { ?item skos:altLabel ?titleAltLabel. }
  FILTER(((LCASE(STR(?titleLabel))) = LCASE(?title)) ||
         ((LCASE(STR(?titleAltLabel))) = LCASE(?title)))

  ?item wdt:P577 ?date.
  FILTER((xsd:integer(YEAR(?date))) = ?year)

  OPTIONAL { ?item wdt:P9751 ?appletv_id. }
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
        expr.list.eval(_escape_str(pl.element())).list.join('" "'),
    )


def find_wd_movie_via_search(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        pl.format(
            _SEARCH_MOVIE_QUERY,
            pl.col("title").pipe(_quote_str),
            pl.col("directors").pipe(_quote_arr_str),
            pl.col("published_at").dt.year(),
            pl.col("published_at").dt.year() + 1,
            pl.col("title").pipe(_quote_str),
        )
        .pipe(sparql_batch, columns=["item", "has_appletv"])
        .alias("results"),
    )


def find_wd_show_via_search(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(
        pl.format(
            _SEARCH_SHOW_QUERY,
            pl.col("title").pipe(_quote_str),
            pl.col("published_at").dt.year(),
            pl.col("published_at").dt.year() + 1,
            pl.col("title").pipe(_quote_str),
        )
        .pipe(sparql_batch, columns=["item", "has_appletv"])
        .alias("results")
    )


_ANY_MOVIE_ID_QUERY = """
SELECT DISTINCT ?id WHERE { ?statement ps:P9586 ?id. }
"""

_ANY_SHOW_ID_QUERY = """
SELECT DISTINCT ?id WHERE { ?statement ps:P9751 ?id. }
"""


_ADD_RDF_MOVIE_STATEMENT = pl.format(
    '<{}> wdt:P9586 "{}" .', pl.col("item"), pl.col("id")
).alias("rdf_statement")

_ADD_RDF_SHOW_STATEMENT = pl.format(
    '<{}> wdt:P9751 "{}" .', pl.col("item"), pl.col("id")
).alias("rdf_statement")


def _find_movie_via_search(sitemap_df: pl.LazyFrame) -> pl.LazyFrame:
    wd_df = (
        sparql(_ANY_MOVIE_ID_QUERY, columns=["id"])
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
            & pl.col("directors").list.len().ge(1)
        )
        .join(wd_df, on="id", how="left", coalesce=True)
        .filter(pl.col("wd_exists").is_null())
        .sort(pl.col("published_at"), descending=True)
        .pipe(weighted_sample, n=_SEARCH_LIMIT)
        .pipe(find_wd_movie_via_search)
        .filter(pl.col("results").list.len() == 1)
        .with_columns(pl.col("results").list.first().alias("result"))
        .unnest("result")
        .filter(pl.col("item").is_not_null() & pl.col("has_appletv").is_null())
        .select(_ADD_RDF_MOVIE_STATEMENT)
    )


def _find_show_via_search(sitemap_df: pl.LazyFrame) -> pl.LazyFrame:
    wd_df = (
        sparql(_ANY_SHOW_ID_QUERY, columns=["id"])
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
        )
        .join(wd_df, on="id", how="left", coalesce=True)
        .filter(pl.col("wd_exists").is_null())
        .sort(pl.col("published_at"), descending=True)
        .pipe(weighted_sample, n=_SEARCH_LIMIT)
        .pipe(find_wd_show_via_search)
        .filter(pl.col("results").list.len() == 1)
        .with_columns(pl.col("results").list.first().alias("result"))
        .unnest("result")
        .filter(pl.col("item").is_not_null() & pl.col("has_appletv").is_null())
        .select(_ADD_RDF_SHOW_STATEMENT)
    )


def _main() -> None:
    pl.enable_string_cache()

    sitemap_movie_df = scan_s3_parquet_anon("s3://wikidatabots/appletv/movie.parquet")
    sitemap_show_df = scan_s3_parquet_anon("s3://wikidatabots/appletv/show.parquet")

    pl.concat(
        [
            _find_movie_via_search(sitemap_movie_df),
            _find_show_via_search(sitemap_show_df),
        ]
    ).pipe(print_rdf_statements, sample=False)


if __name__ == "__main__":
    _main()
