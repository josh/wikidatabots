# pyright: strict

import atexit
import logging
import os
from typing import Any, TypedDict

import requests_cache

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY")

session = requests_cache.CachedSession(
    ".cache/opencritic_requests_cache",
    expire_after=0,
    cache_control=True,
)
session.headers.update({"X-RapidAPI-Host": "opencritic-api.p.rapidapi.com"})

request_count = 0
cache_hit_count = 0


def _track_cache_stats(response: Any):
    global request_count
    global cache_hit_count
    request_count += 1
    if response.from_cache:
        cache_hit_count += 1


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
    _track_cache_stats(response)
    if response.status_code == 429:
        raise RatelimitException(f"Ratelimited exceeded after {request_count} requests")
    response.raise_for_status()
    request_count += 1
    return response.json()


def log_cache_stats():
    session.remove_expired_responses()
    logging.info(f"opencritic requests-cache: {cache_hit_count}/{request_count}")


atexit.register(log_cache_stats)
