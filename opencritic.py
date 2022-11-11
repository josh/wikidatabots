# pyright: strict

import os
from typing import TypedDict

import requests_cache

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY")

session = requests_cache.CachedSession(
    ".cache/opencritic_requests_cache",
    expire_after=0,
    cache_control=True,
)
session.headers.update({"X-RapidAPI-Host": "opencritic-api.p.rapidapi.com"})


class RatelimitException(Exception):
    pass


class OpenCriticGame(TypedDict):
    id: int
    name: str
    url: str
    percentRecommended: float
    numReviews: int
    numTopCriticReviews: int
    medianScore: int
    topCriticScore: float
    percentile: int
    firstReleaseDate: str
    createdAt: str
    updatedAt: str
    firstReviewDate: str
    latestReviewDate: str
    tenthReviewDate: str
    criticalReviewDate: str


request_count = 0


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
