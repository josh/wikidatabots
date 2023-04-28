# pyright: strict

from typing import TypedDict

import polars as pl

import appletv
from sparql import sparql, sparql_df


class WikidataSearchResult(TypedDict):
    qid: str
    appletv: appletv.ID | None


def wikidata_search(
    title: str,
    year: int,
    director: str,
) -> WikidataSearchResult | None:
    query = "SELECT DISTINCT ?item ?appletv WHERE {\n"

    query += """
      SERVICE wikibase:mwapi {
        bd:serviceParam wikibase:endpoint "www.wikidata.org";
                        wikibase:api "EntitySearch";
                        mwapi:search "<<TITLE>>";
                        mwapi:language "en".
        ?item wikibase:apiOutputItem mwapi:item.
      }

      OPTIONAL { ?item rdfs:label ?titleLabel. }
      OPTIONAL { ?item skos:altLabel ?titleAltLabel. }
      FILTER(((LCASE(STR(?titleLabel))) = LCASE("<<TITLE>>")) ||
            ((LCASE(STR(?titleAltLabel))) = LCASE("<<TITLE>>")))
    """.replace(
        "<<TITLE>>", title.replace('"', '\\"')
    )

    years = [year, year - 1]
    query += """
    ?item wdt:P577 ?date.
    """
    query += (
        "FILTER("
        + " || ".join([f"((xsd:integer(YEAR(?date))) = {y} )" for y in years])
        + ")"
    )

    query += """
    ?item wdt:P57 ?director.
    ?director rdfs:label ?directorLabel.
    """
    query += (
        "FILTER("
        + " || ".join(
            [
                '(STR(?directorLabel)) = "{}"'.format(d.replace('"', '\\"'))
                for d in [director]  # TODO: flatten to first name
            ]
        )
        + ")"
    )

    query += """
    VALUES ?classes {
      wd:Q11424
      wd:Q506240
    }
    ?item (wdt:P31/(wdt:P279*)) ?classes.

    OPTIONAL { ?item wdt:P9586 ?appletv }
    """

    query += "\n} LIMIT 2"

    Result = TypedDict("Result", {"item": str, "appletv": str})
    results: list[Result] = sparql(query)

    if len(results) == 1:
        result = results[0]
        qid = result["item"]
        appletv_id = appletv.tryid(result["appletv"])
        return WikidataSearchResult(qid=qid, appletv=appletv_id)
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

        result = wikidata_search(title, year, director)
        if result and not result["appletv"]:
            print(f'wd:{result["qid"]} wdt:P9586 "{id}" .')


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
