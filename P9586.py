import html
import itertools
import json
from collections.abc import Iterable
from typing import Any, TypedDict

import backoff
import pandas as pd
import requests
from bs4 import BeautifulSoup

import appletv
import wikidata
from sparql import sparql
from timeout import iter_until_deadline
from utils import shuffled, tryint

session = requests.Session()


@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_tries=3)
def fetch_movie(url: str) -> tuple[str, int, set[str]] | None:
    soup = appletv.fetch(url)
    if not soup:
        return None

    ld = find_ld(soup)
    if not ld:
        return None

    title: str = html.unescape(ld["name"])

    year = tryint(ld.get("datePublished", "")[0:4])
    if not year:
        return None

    directors: set[str] = set()
    for director in ld.get("director", []):
        directors.add(html.unescape(director["name"]))
    if not directors:
        return None

    return (title, year, directors)


def find_ld(soup: BeautifulSoup) -> dict[str, Any] | None:
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        ld = json.loads(script.string)
        if ld["@type"] == "Movie":
            return ld
    return None


class WikidataSearchResult(TypedDict):
    qid: wikidata.QID
    appletv: appletv.ID | None


def wikidata_search(
    title: str,
    year: int,
    directors: Iterable[str],
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
                for d in directors
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

    Result = TypedDict("Result", item=wikidata.QID, appletv=str)
    results: list[Result] = sparql(query)

    if len(results) == 1:
        result = results[0]
        qid = result["item"]
        appletv_id = appletv.tryid(result["appletv"])
        return WikidataSearchResult(qid=qid, appletv=appletv_id)
    return None


def matched_appletv_ids() -> set[appletv.ID]:
    query = "SELECT DISTINCT ?appletv WHERE { ?statement ps:P9586 ?appletv. }"
    ids: set[appletv.ID] = set()
    for result in sparql(query):
        if id := appletv.tryid(result["appletv"]):
            ids.add(id)
    return ids


def main():
    limit = 500
    skip_ids = matched_appletv_ids()

    sitemap_df = pd.read_feather(
        "s3://wikidatabots/appletv/movie/sitemap.arrow",
        columns=["loc", "country", "id"],
    )
    sitemap_df = sitemap_df[sitemap_df["country"] == "us"]

    def candiate_urls():
        for url in shuffled(appletv.fetch_new_sitemap_urls())[0:250]:
            id = appletv.parse_movie_url(url)
            if not id or id in skip_ids:
                continue
            yield (url, id)

        for row in sitemap_df.sample(250).itertuples():  # type: ignore
            url, id = row.loc, row.id  # type: ignore
            assert isinstance(url, str)
            assert isinstance(id, str)
            if id in skip_ids:
                continue
            yield (url, id)

    print("qid,P9586")
    for (url, id) in iter_until_deadline(itertools.islice(candiate_urls(), limit)):
        info = fetch_movie(url)
        if not info:
            continue
        result = wikidata_search(*info)
        if result and not result["appletv"]:
            print(f'{result["qid"]},"""{id}"""')


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
