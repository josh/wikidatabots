# pyright: strict

from math import floor

import polars as pl

from appletv_etl import REGION_COUNT, not_found
from polars_utils import limit
from sparql import sparql, sparql_batch

_SEARCH_LIMIT = 250
_NOT_FOUND_LIMIT = floor(100 / REGION_COUNT)

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


def _find_movie_via_search() -> pl.LazyFrame:
    wd_df = (
        sparql(_ANY_ID_QUERY, columns=["id"])
        .select(pl.col("id").str.extract("^(umc.cmc.[a-z0-9]{22,25})$"))
        .drop_nulls()
        .with_columns(pl.lit(True).alias("wd_exists"))
    )

    sitemap_df = (
        pl.scan_parquet(
            "s3://wikidatabots/appletv/movie/sitemap.parquet",
            storage_options={"anon": True},
        )
        .filter((pl.col("country") == "us") & pl.col("in_latest_sitemap"))
        .select(["id", "loc"])
    )

    jsonld_df = (
        pl.scan_parquet(
            "s3://wikidatabots/appletv/movie/jsonld.parquet",
            storage_options={"anon": True},
        )
        .filter(pl.col("jsonld_success"))
        .select(["loc", "title", "published_at", "director"])
    )

    sitemap_df = (
        sitemap_df.join(jsonld_df, on="loc", how="left")
        .join(wd_df, on="id", how="left")
        .filter(
            pl.col("wd_exists").is_null()
            & pl.col("title").is_not_null()
            & pl.col("published_at").is_not_null()
            & pl.col("director").is_not_null()
        )
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

    return sitemap_df


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


def _find_movie_not_found() -> pl.LazyFrame:
    return (
        sparql(_ID_QUERY, columns=["statement", "id"])
        .with_columns(
            pl.col("id").str.extract("^(umc.cmc.[a-z0-9]{22,25})$").alias("id"),
        )
        .drop_nulls()
        .select("statement", "id")
        .pipe(limit, soft=_NOT_FOUND_LIMIT, desc="deprecated candidate ids")
        .pipe(not_found, type="movie")
        .filter(pl.col("all_not_found"))
        .select(_DEPRECATE_RDF_STATEMENT)
    )


def main() -> None:
    df = pl.concat(
        [
            _find_movie_via_search(),
            _find_movie_not_found(),
        ]
    )

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
