# pyright: strict

import os
from typing import TypedDict

import requests

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY")

session = requests.Session()
session.headers.update({"X-RapidAPI-Host": "opencritic-api.p.rapidapi.com"})

request_count = 0


class RatelimitException(Exception):
    pass


class OpenCriticGame(TypedDict):
    id: int
    name: str
    url: str
    numReviews: int
    topCriticScore: float
    latestReviewDate: str


def fetch_game(game_id: int, api_key: str | None = RAPIDAPI_KEY) -> OpenCriticGame:
    global request_count
    assert api_key, "No RapidAPI key provided"
    url = f"https://opencritic-api.p.rapidapi.com/game/{game_id}"
    headers = {"X-RapidAPI-Key": api_key}
    response = session.get(url, headers=headers, timeout=5)
    if response.status_code == 429:
        raise RatelimitException(f"Ratelimited exceeded after {request_count} requests")
    response.raise_for_status()
    request_count += 1
    return response.json()
