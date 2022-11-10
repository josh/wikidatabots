# pyright: strict

import os
from typing import TypedDict

import requests

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY")


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


def fetch_game(game_id: int, api_key: str | None = RAPIDAPI_KEY) -> OpenCriticGame:
    assert api_key, "No RapidAPI key provided"
    url = f"https://opencritic-api.p.rapidapi.com/game/{game_id}"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "opencritic-api.p.rapidapi.com",
    }
    response = requests.get(url, headers=headers, timeout=5)
    if response.status_code == 429:
        raise RatelimitException()
    response.raise_for_status()
    return response.json()
