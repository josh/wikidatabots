import csv
import json
import re
import zlib

import requests
from bs4 import BeautifulSoup

import itunes


def movie(id):
    soup = fetch("https://tv.apple.com/us/movie/{}".format(id))
    if not soup:
        return None

    itunes_id = None
    possible_itunes_id = extract_itunes_id(soup)
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
    + "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    + "Version/14.1.1 Safari/605.1.15"
)

request_headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-us",
    "User-Agent": user_agent,
}


def fetch(url):
    r = requests.get(url, headers=request_headers)
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


def all_not_found(type, id):
    assert type in {"movie", "episode", "show"}
    for region in regions:
        url = "https://tv.apple.com/{}/{}/{}".format(region, type, id)
        if not not_found(url=url):
            return False
    return True


def not_found(url):
    r = requests.get(url, headers=request_headers)
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    if soup.find("div", {"class": "not-found"}):
        return True
    else:
        return False


def extract_shoebox(soup):
    script = soup.find("script", {"type": "fastboot/shoebox", "id": "shoebox-uts-api"})
    if not script:
        return []

    boxes = []
    data = json.loads(script.string)
    for key in data:
        subdata = json.loads(data[key])
        if "d" in subdata and "data" in subdata["d"]:
            boxes.append(subdata["d"]["data"])
    return boxes


def extract_itunes_id(soup):
    for data in extract_shoebox(soup):
        if "content" in data and "playables" in data["content"]:
            for playable in data["content"]["playables"]:
                if playable.get("isItunes", False) is True:
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


def fetch_sitemap_index_urls():
    r = requests.get("https://tv.apple.com/sitemaps_tv_index_1.xml")
    r.raise_for_status()

    soup = BeautifulSoup(r.content, "xml")

    urls = set()
    for loc in soup.find_all("loc"):
        urls.add(loc.text)
    return urls


def fetch_sitemap_index(url):
    r = requests.get(url)
    r.raise_for_status()

    xml = zlib.decompress(r.content, 16 + zlib.MAX_WBITS)
    soup = BeautifulSoup(xml, "xml")

    urls = set()
    for loc in soup.find_all("loc"):
        urls.add(loc.text)
    for link in soup.find_all("xhtml:link"):
        urls.add(link["href"])
    return urls


def fetch_new_sitemap_urls():
    r = requests.get("https://github.com/josh/mixnmatch-catalogs/commit/main.diff")
    r.encoding = "utf-8"
    r.raise_for_status()

    def new_rows():
        for line in r.iter_lines(decode_unicode=True):
            if line and line.startswith("+umc.cmc"):
                yield line[1:]

    for (id, name, desc, url, type) in csv.reader(new_rows()):
        if name:
            yield url
