import os
from typing import Any, Literal, Optional

import backoff
import requests

TMDB_API_KEY = os.environ.get("TMDB_API_KEY")


class UnauthorizedException(Exception):
    pass


@backoff.on_exception(backoff.expo, requests.exceptions.ConnectionError, max_tries=3)
def api_request(
    path: str,
    params: dict[str, str] = {},
    version: int = 3,
    api_key: Optional[str] = TMDB_API_KEY,
) -> dict[str, Any]:
    url = "https://api.themoviedb.org/{}{}".format(str(version), path)
    headers = {"Accept-Encoding": "identity"}
    post_params: dict[str, str] = {}
    if api_key:
        post_params["api_key"] = api_key
    post_params.update(params)

    # try:
    r = requests.get(url, headers=headers, params=post_params)
    # except requests.exceptions.ContentDecodingError:
    #     return {}

    if r.headers["Content-Type"].startswith("application/json"):
        data = r.json()
        if r.status_code == 401:
            raise UnauthorizedException(data["status_message"])
        return data
    else:
        r.raise_for_status()
        return {}


ObjectType = Literal["movie", "tv", "person"]
object_types: set[ObjectType] = set(["movie", "tv", "person"])

ObjectResult = dict[str, Any]


def object(
    id: int,
    type: ObjectType,
    append: list[str] = [],
    api_key: Optional[str] = TMDB_API_KEY,
) -> Optional[ObjectResult]:
    assert type in object_types

    params: dict[str, str] = {}
    if append:
        params["append_to_response"] = ",".join(append)

    resp = api_request(
        "/{}/{}".format(type, id),
        params=params,
        api_key=api_key,
    )
    if resp.get("success") is False:
        return None
    return resp


FindSource = Literal[
    "imdb_id",
    "freebase_mid",
    "freebase_id",
    "tvdb_id",
    "tvrage_id",
    "facebook_id",
    "twitter_id",
    "instagram_id",
]
find_sources: set[FindSource] = set(
    [
        "imdb_id",
        "freebase_mid",
        "freebase_id",
        "tvdb_id",
        "tvrage_id",
        "facebook_id",
        "twitter_id",
        "instagram_id",
    ]
)

FindType = Literal["movie", "person", "tv", "tv_episode", "tv_season"]
find_types: set[FindType] = set(["movie", "person", "tv", "tv_episode", "tv_season"])

FindResult = dict[str, Any]


def find(
    id: str,
    source: FindSource,
    type: FindType,
    api_key: Optional[str] = TMDB_API_KEY,
) -> Optional[FindResult]:
    assert source in find_sources
    assert type in find_types

    resp = api_request(
        "/find/{}".format(id),
        params={"external_source": source},
        api_key=api_key,
    )

    results = resp.get("{}_results".format(type))
    if results and len(results) == 1:
        return results[0]
    else:
        return None
