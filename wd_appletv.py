# pyright: strict


import polars as pl

from sparql import sparql_df

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


def wikidata_search(title: str, year: int, director: str) -> str | None:
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


_ID_QUERY = """
SELECT DISTINCT ?id WHERE { ?statement ps:P9586 ?id. }
"""


def main() -> None:
    limit = 500

    wd_df = (
        sparql_df(_ID_QUERY, columns=["id"])
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
        .select("id", "title", "published_at", "director")
        .collect()
    )

    for row in sitemap_df.sample(limit).iter_rows(named=True):
        id = row["id"]
        title = row["title"]
        year = row["published_at"].year
        director = row["director"]

        assert isinstance(title, str)
        assert isinstance(year, int)
        assert isinstance(director, str)

        if item := wikidata_search(title, year, director):
            print(f'<{item}> wdt:P9586 "{id}" .')


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
