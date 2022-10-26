import re
from typing import NewType, Optional
from urllib.parse import urlparse

import requests

ID = NewType("ID", str)


def imdb_id(id: str) -> ID:
    assert formatted_url(id), f"'{id}' is not a valid IMDb ID"
    return ID(id)


def parse_imdb_id(id: str) -> Optional[ID]:
    if formatted_url(id):
        return ID(id)
    return None


def canonical_id(id: ID) -> Optional[ID]:
    url = formatted_url(id)
    assert url, "bad id: {}".format(id)

    if id.startswith("ch") or id.startswith("ev"):
        return id

    r = requests.head(url)
    if r.status_code == 200:
        return id
    elif r.status_code == 301 or r.status_code == 308:
        new_url = r.headers["Location"]
        new_id = extract_id(new_url)
        assert new_id, "redirect bad id: {}".format(new_url)
        return new_id
    elif r.status_code == 404:
        return None
    else:
        assert "unhandled imdb status code: {}".format(r.status_code)


def formatted_url(id: str) -> Optional[str]:
    m = re.fullmatch(r"(tt\d+)", id)
    if m:
        return f"https://www.imdb.com/title/{m.group(1)}/"

    m = re.fullmatch(r"(nm\d+)", id)
    if m:
        return f"https://www.imdb.com/name/{m.group(1)}/"

    m = re.fullmatch(r"(ch\d+)", id)
    if m:
        return f"https://www.imdb.com/character/{m.group(1)}/"

    m = re.fullmatch(r"(ev\d+)/(\d+)/(\d+)", id)
    if m:
        return f"https://www.imdb.com/event/{m.group(1)}/{m.group(2)}/{m.group(3)}"

    m = re.fullmatch(r"(ev\d+)/(\d+)", id)
    if m:
        return f"https://www.imdb.com/event/{m.group(1)}/{m.group(2)}/1"

    m = re.fullmatch(r"(ev\d+)", id)
    if m:
        return f"https://www.imdb.com/event/{m.group(1)}"

    m = re.fullmatch(r"(co\d+)", id)
    if m:
        return f"https://www.imdb.com/search/title/?companies={m.group(1)}"

    return None


def extract_id(url: str) -> Optional[ID]:
    r = urlparse(url)

    if r.netloc != "www.imdb.com" and r.netloc != "":
        return None

    m = re.match(r"/title/(tt\d+)/?$", r.path)
    if m:
        return ID(m.group(1))

    m = re.match(r"/name/(nm\d+)/?$", r.path)
    if m:
        return ID(m.group(1))

    m = re.match(r"/character/(ch\d+)/?$", r.path)
    if m:
        return ID(m.group(1))

    m = re.match(r"/event/(ev\d+)/(\d+)(/|/\d+)?$", r.path)
    if m:
        return ID(f"{m.group(1)}/{m.group(2)}")

    m = re.match(r"/event/(ev\d+)/?$", r.path)
    if m:
        return ID(m.group(1))

    m = re.match(r"/company/(co\d+)/?$", r.path)
    if m:
        return ID(m.group(1))

    if r.path == "/search/title/":
        m = re.match(r"companies=(co\d+)", r.query)
        if m:
            return ID(m.group(1))

    return None
