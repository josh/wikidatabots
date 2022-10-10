import html
import itertools
import json
import re
from typing import Any, Iterable, Literal, Optional, TypedDict

import backoff
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

import appletv
from page import page_statements
from sparql import sparql
from utils import shuffled, tryint


def parseurl(
    url: str,
) -> tuple[Literal["movie"], str] | tuple[Literal["unknown"], None]:
    m = re.match(
        r"https://tv.apple.com/us/(movie)/([^/]+/)?(umc.cmc.[0-9a-z]+)",
        url,
    )
    if m:
        assert m.group(1) == "movie"
        return ("movie", m.group(3))
    return ("unknown", None)


@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_tries=3)
def fetch_movie(url: str) -> Optional[tuple[str, int, set[str]]]:
    r = requests.get(url, headers=appletv.request_headers)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

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


def find_ld(soup: BeautifulSoup) -> Optional[dict[str, Any]]:
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        ld = json.loads(script.string)
        if ld["@type"] == "Movie":
            return ld
    return None


class WikidataSearchResult(TypedDict):
    item: str
    appletv: Optional[str]


def wikidata_search(
    title: str,
    year: int,
    directors: Iterable[str],
) -> Optional[WikidataSearchResult]:
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
        + " || ".join(["((xsd:integer(YEAR(?date))) = {} )".format(y) for y in years])
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

    results = sparql(query)
    if len(results) == 1:
        result = results[0]
        qid: str = result["item"]
        appletv: Optional[str] = result["appletv"]
        return WikidataSearchResult(item=qid, appletv=appletv)
    return None


def matched_appletv_ids() -> set[str]:
    query = "SELECT DISTINCT ?appletv WHERE { ?statement ps:P9586 ?appletv. }"
    ids: set[str] = set()
    for result in sparql(query):
        ids.add(result["appletv"])
    return ids


def main():
    limit = 500
    skip_ids = matched_appletv_ids()
    page_title = "User:Josh404Bot/Preliminarily matched/P9586"

    def candiate_urls():
        for (_item, property, id) in page_statements(page_title):
            if property != "P9586":
                continue
            if not id or id in skip_ids:
                continue
            url = "https://tv.apple.com/us/movie/{}".format(id)
            yield (url, id)

        for url in shuffled(appletv.fetch_new_sitemap_urls())[0:250]:
            (type, id) = parseurl(url)
            if type != "movie":
                continue
            if not id or id in skip_ids:
                continue
            yield (url, id)

        for index_url in shuffled(appletv.fetch_sitemap_index_urls())[0:250]:
            for url in shuffled(appletv.fetch_sitemap_index(index_url)):
                (type, id) = parseurl(url)
                if type != "movie":
                    continue
                if not id or id in skip_ids:
                    continue
                yield (url, id)

    print("qid,P9586")
    for (url, id) in tqdm(itertools.islice(candiate_urls(), limit), total=limit):
        info = fetch_movie(url)
        if not info:
            continue
        result = wikidata_search(*info)
        if result and not result["appletv"]:
            print(f'{result["item"]},"""{id}"""')


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
