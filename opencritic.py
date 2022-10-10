import os
from typing import Optional, TypedDict
import requests

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY")


class OpenCriticGame(TypedDict):
    name: str
    numReviews: int
    topCriticScore: float


def fetch_game(game_id: int, api_key: Optional[str] = RAPIDAPI_KEY) -> OpenCriticGame:
    assert api_key, "No RapidAPI key provided"
    url = f"https://opencritic-api.p.rapidapi.com/game/{game_id}"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "opencritic-api.p.rapidapi.com",
    }
    response = requests.get(url, headers=headers, timeout=5)
    return response.json()
