# pyright: strict

import json
import re
from typing import Any, Literal, NewType

from bs4 import BeautifulSoup

ID = NewType("ID", str)
IDPattern = re.compile("umc.cmc.[a-z0-9]{22,25}")


def id(id: str) -> ID:
    assert re.fullmatch(IDPattern, id), f"'{id}' is an invalid Apple TV ID"
    return ID(id)


def tryid(id: Any) -> ID | None:
    if type(id) is str and re.fullmatch(IDPattern, id):
        return ID(id)
    return None


Type = Literal["movie", "episode", "show"]


def has_not_found_text(text: str) -> bool:
    soup = BeautifulSoup(text, "html.parser")
    if soup.find("div", {"class": "not-found"}):
        return True
    else:
        return False


def _extract_shoebox(soup: BeautifulSoup) -> list[Any]:
    script = soup.find("script", {"type": "fastboot/shoebox", "id": "shoebox-uts-api"})
    if not script:
        return []

    return json.loads(script.text).values()


def extract_itunes_id(text: str) -> int | None:
    soup = BeautifulSoup(text, "html.parser")

    if soup.find("h1", string="This content is no longer available."):
        return None

    link = soup.find("link", attrs={"rel": "canonical"})
    if not link:
        return None

    for data in _extract_shoebox(soup):
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
