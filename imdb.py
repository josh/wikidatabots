import re
from urllib.parse import urlparse

import requests


def is_valid_id(id):
    if formatted_url(id):
        return True
    else:
        return False


def canonical_id(id):
    url = formatted_url(id)
    assert url, "bad id: {}".format(id)

    if id.startswith("ch") or id.startswith("ev"):
        return id

    r = requests.head(url)
    if r.status_code == 200:
        return id
    elif r.status_code == 301:
        new_url = r.headers["Location"]
        new_id = extract_id(new_url)
        assert new_id, "redirect bad id: {}".format(new_url)
        return new_id
    elif r.status_code == 404:
        return None
    else:
        assert "unhandled imdb status code: {}".format(r.status_code)


def formatted_url(id):
    m = re.match(r"^(tt\d+)$", id)
    if m:
        return "https://www.imdb.com/title/{}/".format(m.group(1))

    m = re.match(r"^(nm\d+)$", id)
    if m:
        return "https://www.imdb.com/name/{}/".format(m.group(1))

    m = re.match(r"^(ch\d+)$", id)
    if m:
        return "https://www.imdb.com/character/{}/".format(m.group(1))

    m = re.match(r"^(ev\d+)/(\d+)$", id)
    if m:
        return "https://www.imdb.com/event/{}/{}/1".format(m.group(1), m.group(2))

    m = re.match(r"^(ev\d+)$", id)
    if m:
        return "https://www.imdb.com/event/{}".format(m.group(1))

    m = re.match(r"^(co\d+)$", id)
    if m:
        return "https://www.imdb.com/search/title/?companies={}".format(m.group(1))

    return None


def extract_id(url):
    r = urlparse(url)

    if r.netloc != "www.imdb.com" and r.netloc != "":
        return None

    m = re.match(r"/title/(tt\d+)/?$", r.path)
    if m:
        return m.group(1)

    m = re.match(r"/name/(nm\d+)/?$", r.path)
    if m:
        return m.group(1)

    m = re.match(r"/character/(ch\d+)/?$", r.path)
    if m:
        return m.group(1)

    m = re.match(r"/event/(ev\d+)/(\d+)(/|/\d+)?$", r.path)
    if m:
        return "{}/{}".format(m.group(1), m.group(2))

    m = re.match(r"/event/(ev\d+)/?$", r.path)
    if m:
        return "{}".format(m.group(1))

    m = re.match(r"/company/(co\d+)/?$", r.path)
    if m:
        return m.group(1)

    if r.path == "/search/title/":
        m = re.match(r"companies=(co\d+)", r.query)
        if m:
            return m.group(1)

    return None
