# pyright: strict

from math import floor

import polars as pl

from appletv_etl import REGION_COUNT, not_found, url_extract_id
from polars_utils import limit
from sparql import sparql, sparql_batch

_SEARCH_LIMIT = 250
_NOT_FOUND_LIMIT = floor(100 / REGION_COUNT)
_STATEMENT_LIMIT = (100, 10_000)

_SEARCH_QUERY = """
SELECT DISTINCT ?item ?has_appletv WHERE {
  VALUES ?title { "{}" }
  VALUES ?directorName { "{}" }
  VALUES ?year { {} {} }

  SERVICE wikibase:mwapi {
    bd:serviceParam wikibase:endpoint "www.wikidata.org";
                    wikibase:api "EntitySearch";
                    mwapi:search "{}";
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

  OPTIONAL { ?item wdt:P9586 ?has_appletv. }
}
LIMIT 2
"""

_ANY_ID_QUERY = """
SELECT DISTINCT ?id WHERE { ?statement ps:P9586 ?id. }
"""

_ADD_RDF_STATEMENT = pl.format(
    '<{}> wdt:P9586 "{}" .', pl.col("item"), pl.col("id")
).alias("rdf_statement")


def _find_movie_via_search(sitemap_df: pl.LazyFrame) -> pl.LazyFrame:
    wd_df = (
        sparql(_ANY_ID_QUERY, columns=["id"])
        .select(pl.col("id").str.extract("^(umc.cmc.[a-z0-9]{22,25})$"))
        .drop_nulls()
        .with_columns(pl.lit(True).alias("wd_exists"))
    )

    return (
        sitemap_df.filter(
            (pl.col("country") == "us")
            & pl.col("in_latest_sitemap")
            & pl.col("jsonld_success")
            & pl.col("title").is_not_null()
            & pl.col("published_at").is_not_null()
            & pl.col("director").is_not_null()
        )
        .join(wd_df, on="id", how="left")
        .filter(pl.col("wd_exists").is_null())
        .pipe(limit, soft=_SEARCH_LIMIT, desc="unmatched sitemap ids")
        .with_columns(
            pl.format(
                _SEARCH_QUERY,
                pl.col("title").str.replace_all('"', '\\"', literal=True),
                pl.col("director").str.replace_all('"', '\\"', literal=True),
                pl.col("published_at").dt.year(),
                pl.col("published_at").dt.year() + 1,
                pl.col("title").str.replace_all('"', '\\"', literal=True),
            )
            .pipe(sparql_batch, columns=["item", "has_appletv"])
            .alias("result"),
        )
        .filter(pl.col("result").arr.lengths() == 1)
        .with_columns(
            pl.col("result").arr.first().alias("result"),
        )
        .unnest("result")
        .filter(pl.col("item").is_not_null() & pl.col("has_appletv").is_null())
        .select(_ADD_RDF_STATEMENT)
    )


_ID_QUERY = """
SELECT ?statement ?id WHERE {
  ?statement ps:P9586 ?id.
  ?statement wikibase:rank ?rank.
  FILTER(?rank != wikibase:DeprecatedRank)
}
"""

_DEPRECATE_RDF_STATEMENT = pl.format(
    "<{}> wikibase:rank wikibase:DeprecatedRank ; pq:P2241 wd:Q21441764 ; "
    'wikidatabots:editSummary "Deprecate Apple TV movie ID delisted from store" .',
    pl.col("statement"),
).alias("rdf_statement")


def _find_movie_not_found(sitemap_df: pl.LazyFrame) -> pl.LazyFrame:
    sitemap_df = (
        sitemap_df.select("id", "in_latest_sitemap")
        .groupby("id")
        .agg(pl.col("in_latest_sitemap").any())
    )

    return (
        sparql(_ID_QUERY, columns=["statement", "id"])
        .with_columns(
            pl.col("id").str.extract("^(umc.cmc.[a-z0-9]{22,25})$").alias("id"),
        )
        .drop_nulls()
        .select("statement", "id")
        .join(sitemap_df, on="id", how="left")
        .filter(
            pl.col("in_latest_sitemap").is_not() | pl.col("in_latest_sitemap").is_null()
        )
        .pipe(limit, soft=_NOT_FOUND_LIMIT, desc="deprecated candidate ids")
        .pipe(not_found, type="movie")
        .filter(pl.col("all_not_found"))
        .select(_DEPRECATE_RDF_STATEMENT)
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
        .filter(pl.col("appletv_id").is_not_null())
        .select(_ADD_VIA_ITUNES_STATEMENT)
    )


def main() -> None:
    sitemap_df = pl.scan_parquet(
        "s3://wikidatabots/appletv/movie/sitemap.parquet",
        storage_options={"anon": True},
    )
    jsonld_df = pl.scan_parquet(
        "s3://wikidatabots/appletv/movie/jsonld.parquet",
        storage_options={"anon": True},
    )
    sitemap_df = sitemap_df.join(jsonld_df, on="loc", how="left").cache()

    itunes_df = pl.scan_parquet(
        "s3://wikidatabots/itunes.parquet",
        storage_options={"anon": True},
    )

    df = pl.concat(
        [
            _find_movie_via_search(sitemap_df),
            _find_movie_not_found(sitemap_df),
            _find_movie_via_itunes_redirect(itunes_df),
        ]
    ).pipe(limit, _STATEMENT_LIMIT, sample=False, desc="rdf_statements")

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
