import csv
import json
import re
import zlib
from collections.abc import Generator
from typing import Any, Literal, NewType, TypedDict

import requests
from bs4 import BeautifulSoup

import itunes

session = requests.Session()

ID = NewType("ID", str)
IDPattern = re.compile("umc.cmc.[a-z0-9]{22,25}")


def id(id: str) -> ID:
    assert re.fullmatch(IDPattern, id), f"'{id}' is an invalid Apple TV ID"
    return ID(id)


def tryid(id: Any) -> ID | None:
    if type(id) is str and re.fullmatch(IDPattern, id):
        return ID(id)
    return None


class MovieDict(TypedDict):
    id: ID
    itunes_id: itunes.ID | None


def movie(id: ID) -> MovieDict | None:
    soup = fetch(f"https://tv.apple.com/us/movie/{id}")
    if not soup:
        return None

    itunes_id: itunes.ID | None = None
    possible_itunes_id: itunes.ID | None = extract_itunes_id(soup)
    if possible_itunes_id:
        for (id2, result) in itunes.batch_lookup([possible_itunes_id]):
            if result:
                itunes_id = id2

    return {
        "id": id,
        "itunes_id": itunes_id,
    }


user_agent = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/14.1.1 Safari/605.1.15"
)

request_headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-us",
    "User-Agent": user_agent,
}


def fetch(url: str) -> BeautifulSoup | None:
    r = session.get(url, headers=request_headers)
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    if soup.find("h1", string="This content is no longer available."):
        return None

    link = soup.find("link", attrs={"rel": "canonical"})
    if not link:
        return None

    return soup


regions = ["us", "gb", "au", "br", "de", "ca", "it", "es", "fr", "jp", "jp", "cn"]

Type = Literal["movie", "episode", "show"]


def all_not_found(type: Type, id: ID) -> bool:
    for region in regions:
        url = f"https://tv.apple.com/{region}/{type}/{id}"
        if not not_found(url=url):
            return False
    return True


def not_found(url: str) -> bool:
    r = session.get(url, headers=request_headers)
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    if soup.find("div", {"class": "not-found"}):
        return True
    else:
        return False


def extract_shoebox(soup: BeautifulSoup) -> list[Any]:
    script = soup.find("script", {"type": "fastboot/shoebox", "id": "shoebox-uts-api"})
    if not script:
        return []

    return json.loads(script.text).values()


def extract_itunes_id(soup: BeautifulSoup) -> itunes.ID | None:
    for data in extract_shoebox(soup):
        if "content" in data and "playables" in data["content"]:
            for playable in data["content"]["playables"]:
                if playable.get("isItunes", False) is True:
                    return int(playable["externalId"])

        if "playables" in data:
            for playable in data["playables"].values():
                if playable["channelId"] == "tvs.sbd.9001":
                    return int(playable["externalId"])

        if "howToWatch" in data:
            for way in data["howToWatch"]:
                if way["channelId"] != "tvs.sbd.9001":
                    continue

                if way.get("punchoutUrls"):
                    m = re.match(
                        r"itmss://itunes.apple.com/us/[^/]+/[^/]+/id(\d+)",
                        way["punchoutUrls"]["open"],
                    )
                    if m:
                        return int(m.group(1))

                if way.get("versions"):
                    for version in way["versions"]:
                        m = re.match(
                            r"tvs.sbd.9001:(\d+)",
                            version["playableId"],
                        )
                        if m:
                            return int(m.group(1))

    return None


def fetch_sitemap_index_urls() -> Generator[str, None, None]:
    # yield from fetch_sitemap_index_url(
    #     "http://tv.apple.com/sitemaps_tv_index_episode_1.xml"
    # )
    yield from fetch_sitemap_index_url(
        "http://tv.apple.com/sitemaps_tv_index_movie_1.xml"
    )
    yield from fetch_sitemap_index_url(
        "http://tv.apple.com/sitemaps_tv_index_show_1.xml"
    )


def fetch_sitemap_index_url(url: str) -> set[str]:
    r = session.get(url)
    r.raise_for_status()

    soup = BeautifulSoup(r.content, "xml")

    urls: set[str] = set()
    for loc in soup.find_all("loc"):
        urls.add(loc.text)
    return urls


def fetch_sitemap_index(url: str) -> set[str]:
    r = session.get(url)
    r.raise_for_status()

    xml = zlib.decompress(r.content, 16 + zlib.MAX_WBITS)
    soup = BeautifulSoup(xml, "xml")

    urls: set[str] = set()
    for loc in soup.find_all("loc"):
        urls.add(loc.text)
    for link in soup.find_all("xhtml:link"):
        urls.add(link["href"])
    return urls


def fetch_new_sitemap_urls() -> Generator[str, None, None]:
    r = requests.get("https://github.com/josh/mixnmatch-catalogs/commit/main.diff")
    r.encoding = "utf-8"
    r.raise_for_status()

    def new_rows() -> Generator[str, None, None]:
        for line in r.iter_lines(decode_unicode=True):
            if line and line.startswith("+umc.cmc"):
                yield line[1:]

    for (_id, name, _desc, url, _type) in csv.reader(new_rows()):
        if name:
            yield url
