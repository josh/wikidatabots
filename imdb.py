# pyright: strict

import re
from urllib.parse import urlparse

import requests


class CaptchaException(Exception):
    pass


def canonical_id(id: str) -> str | None:
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
    elif r.status_code == 403:
        raise CaptchaException()
    elif r.status_code == 405 and r.headers.get("x-amzn-waf-action") == "captcha":
        raise CaptchaException()
    else:
        assert f"unhandled imdb status code: {r.status_code}"


def formatted_url(id: str) -> str | None:
    if m := re.fullmatch(r"(tt\d+)", id):
        return f"https://www.imdb.com/title/{m.group(1)}/"

    if m := re.fullmatch(r"(nm\d+)", id):
        return f"https://www.imdb.com/name/{m.group(1)}/"

    if m := re.fullmatch(r"(ch\d+)", id):
        return f"https://www.imdb.com/character/{m.group(1)}/"

    if m := re.fullmatch(r"(ev\d+)/(\d+)/(\d+)", id):
        return f"https://www.imdb.com/event/{m.group(1)}/{m.group(2)}/{m.group(3)}"

    if m := re.fullmatch(r"(ev\d+)/(\d+)", id):
        return f"https://www.imdb.com/event/{m.group(1)}/{m.group(2)}/1"

    if m := re.fullmatch(r"(ev\d+)", id):
        return f"https://www.imdb.com/event/{m.group(1)}"

    if m := re.fullmatch(r"(co\d+)", id):
        return f"https://www.imdb.com/search/title/?companies={m.group(1)}"

    return None


def extract_id(url: str) -> str | None:
    r = urlparse(url)

    if r.netloc != "www.imdb.com" and r.netloc != "":
        return None

    if m := re.match(r"/title/(tt\d+)/?$", r.path):
        return str(m.group(1))

    if m := re.match(r"/name/(nm\d+)/?$", r.path):
        return str(m.group(1))

    if m := re.match(r"/character/(ch\d+)/?$", r.path):
        return str(m.group(1))

    if m := re.match(r"/event/(ev\d+)/(\d+)(/|/\d+)?$", r.path):
        return str(f"{m.group(1)}/{m.group(2)}")

    if m := re.match(r"/event/(ev\d+)/?$", r.path):
        return str(m.group(1))

    if m := re.match(r"/company/(co\d+)/?$", r.path):
        return str(m.group(1))

    if r.path == "/search/title/":
        if m := re.match(r"companies=(co\d+)", r.query):
            return str(m.group(1))

    return None
