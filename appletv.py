# pyright: strict

import csv
import html
import json
import re
from collections.abc import Iterator
from datetime import date
from typing import Any, Literal, NewType, TypedDict

import backoff
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


def parse_movie_url(url: str) -> ID | None:
    m = re.match(
        r"https://tv.apple.com/us/(movie)/([^/]+/)?(umc.cmc.[0-9a-z]+)",
        url,
    )
    if m:
        assert m.group(1) == "movie"
        return ID(m.group(3))
    return None


def appletv_to_itunes(appletv_id: ID) -> itunes.ID | None:
    soup = fetch(f"https://tv.apple.com/us/movie/{appletv_id}")
    if not soup:
        return None

    possible_itunes_id = extract_itunes_id(soup)
    if not possible_itunes_id:
        return None

    for (itunes_id, result) in itunes.batch_lookup([possible_itunes_id]):
        if result:
            return itunes_id

    return None


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


@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_tries=3)
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


def extract_jsonld(soup: BeautifulSoup) -> dict[str, Any] | None:
    scripts = soup.find_all("script", {"type": "application/ld+json"})

    for script in scripts:
        data = json.loads(script.text)
        assert "@context" in data, script.text
        assert "@type" in data, script.text
        return data

    return None


class LinkedData(TypedDict):
    url: str
    success: bool
    title: str | None
    published_at: date | None
    director: str | None


def fetch_jsonld(url: str) -> LinkedData:
    data: LinkedData = {
        "url": url,
        "success": False,
        "title": None,
        "published_at": None,
        "director": None,
    }

    soup = fetch(url)
    if not soup:
        return data

    jsonld = extract_jsonld(soup)
    if not jsonld:
        return data

    data["success"] = True

    if isinstance(jsonld.get("name"), str):
        data["title"] = html.unescape(jsonld["name"])

    if isinstance(jsonld.get("datePublished"), str):
        data["published_at"] = date.fromisoformat(jsonld["datePublished"][0:10])

    for person in jsonld.get("director", []):
        if isinstance(person.get("name"), str):
            data["director"] = html.unescape(person["name"])
            break

    return data


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


def fetch_new_sitemap_urls() -> Iterator[str]:
    r = requests.get("https://github.com/josh/mixnmatch-catalogs/commit/main.diff")
    r.encoding = "utf-8"
    r.raise_for_status()

    def new_rows() -> Iterator[str]:
        for line in r.iter_lines(decode_unicode=True):
            if line and line.startswith("+umc.cmc"):
                yield line[1:]

    for (_id, name, _desc, url, _type) in csv.reader(new_rows()):
        if name:
            yield url
