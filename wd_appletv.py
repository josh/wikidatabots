# pyright: strict

from typing import TypedDict

import polars as pl

from appletv_etl import not_found
from polars_utils import apply_with_tqdm, sample
from sparql import sparql_df

_SEARCH_LIMIT = 250
_NOT_FOUND_LIMIT = 25


_SEARCH_QUERY = """
SELECT DISTINCT ?item ?appletv WHERE {
  SERVICE wikibase:mwapi {
    bd:serviceParam wikibase:endpoint "www.wikidata.org";
                    wikibase:api "EntitySearch";
                    mwapi:search "<<TITLE>>";
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
  FILTER(((LCASE(STR(?titleLabel))) = LCASE("<<TITLE>>")) ||
        ((LCASE(STR(?titleAltLabel))) = LCASE("<<TITLE>>")))

  ?item wdt:P577 ?date.
  FILTER(
    ((xsd:integer(YEAR(?date))) = <<YEAR>> ) ||
    ((xsd:integer(YEAR(?date))) = <<NEXT_YEAR>> )
  )

  ?item wdt:P57 ?director.
  ?director rdfs:label ?directorLabel.
  FILTER((STR(?directorLabel)) = "<<DIRECTOR>>")

  OPTIONAL { ?item wdt:P9586 ?appletv. }
}
LIMIT 2
"""


class _SearchInput(TypedDict):
    title: str
    year: int
    director: str


def _wikidata_search(row: _SearchInput) -> str | None:
    title = row["title"]
    year = row["year"]
    director = row["director"]

    query = (
        _SEARCH_QUERY.replace("<<TITLE>>", title.replace('"', '\\"'))
        .replace("<<YEAR>>", str(year))
        .replace("<<NEXT_YEAR>>", str(year + 1))
        .replace("<<DIRECTOR>>", director.replace('"', '\\"'))
    )
    df = (
        sparql_df(query, columns=["item", "appletv"])
        .with_columns(
            (pl.count() == 1).alias("exclusive"),
            pl.col("appletv").is_null().all().alias("no_appletv"),
        )
        .filter(pl.col("exclusive") & pl.col("no_appletv"))
        .select("item")
        .collect()
    )
    if len(df):
        return df.item()
    return None


_ANY_ID_QUERY = """
SELECT DISTINCT ?id WHERE { ?statement ps:P9586 ?id. }
"""

_ADD_RDF_STATEMENT = pl.format(
    '<{}> wdt:P9586 "{}" .', pl.col("item"), pl.col("id")
).alias("rdf_statement")


def _find_movie_via_search() -> pl.LazyFrame:
    wd_df = (
        sparql_df(_ANY_ID_QUERY, columns=["id"])
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
        .pipe(sample, n=_SEARCH_LIMIT)
        .with_columns(
            pl.struct(
                pl.col("title"),
                pl.col("published_at").dt.year().alias("year"),
                pl.col("director"),
            )
            .pipe(
                apply_with_tqdm,
                _wikidata_search,
                return_dtype=pl.Utf8,
                log_group="wikidata_search",
            )
            .alias("item"),
        )
        .filter(pl.col("item").is_not_null())
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
        sparql_df(_ID_QUERY, columns=["statement", "id"])
        .with_columns(
            pl.col("id").str.extract("^(umc.cmc.[a-z0-9]{22,25})$").alias("id"),
        )
        .drop_nulls()
        .select("statement", "id")
        .pipe(sample, n=_NOT_FOUND_LIMIT)
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
