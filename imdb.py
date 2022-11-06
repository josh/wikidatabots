import re
from typing import Any, NewType
from urllib.parse import urlparse

import requests

ID = NewType("ID", str)


def id(id: str) -> ID:
    assert formatted_url(id), f"'{id}' is not a valid IMDb ID"
    return ID(id)


def tryid(id: Any) -> ID | None:
    if type(id) is str and formatted_url(id):
        return ID(id)
    return None


def canonical_id(id: ID) -> ID | None:
    url = formatted_url(id)
    assert url, f"bad id: {id}"

    if id.startswith("ch") or id.startswith("ev"):
        return id

    r = requests.head(url)
    if r.status_code == 200:
        return id
    elif r.status_code == 301 or r.status_code == 308:
        new_url = r.headers["Location"]
        new_id = extract_id(new_url)
        assert new_id, f"redirect bad id: {new_url}"
        return new_id
    elif r.status_code == 404:
        return None
    else:
        assert f"unhandled imdb status code: {r.status_code}"


def formatted_url(id: str) -> str | None:
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


def extract_id(url: str) -> ID | None:
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
