# pyright: strict

import datetime
import os
from collections.abc import Iterable
from typing import Any, Iterator, Literal

import backoff
import requests
import requests_cache

TMDB_API_KEY = os.environ.get("TMDB_API_KEY")

session = requests_cache.CachedSession(
    ".cache/tmdb_requests_cache",
    expire_after=0,
    cache_control=True,
    ignored_parameters=["api_key"],
)


class UnauthorizedException(Exception):
    pass


@backoff.on_exception(backoff.expo, requests.exceptions.ConnectionError, max_tries=3)
def api_request(
    path: str,
    params: dict[str, str] = {},
    version: int = 3,
    api_key: str | None = TMDB_API_KEY,
) -> dict[str, Any]:
    url = f"https://api.themoviedb.org/{str(version)}{path}"
    headers = {"Accept-Encoding": "identity"}
    post_params: dict[str, str] = {}
    if api_key:
        post_params["api_key"] = api_key
    post_params.update(params)

    r = session.get(url, headers=headers, params=post_params)

    if r.headers.get("Content-Type", "").startswith("application/json"):
        data = r.json()
        if r.status_code == 401:
            raise UnauthorizedException(data["status_message"])
        return data
    else:
        r.raise_for_status()
        return {}


ObjectType = Literal["movie", "tv", "person"]
object_types: set[ObjectType] = {"movie", "tv", "person"}

ObjectResult = dict[str, Any]


def object(
    id: int,
    type: ObjectType,
    append: Iterable[str] = [],
    api_key: str | None = TMDB_API_KEY,
) -> ObjectResult | None:
    assert type in object_types

    params: dict[str, str] = {}
    if append:
        params["append_to_response"] = ",".join(append)

    resp = api_request(
        f"/{type}/{id}",
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
find_sources: set[FindSource] = {
    "imdb_id",
    "freebase_mid",
    "freebase_id",
    "tvdb_id",
    "tvrage_id",
    "facebook_id",
    "twitter_id",
    "instagram_id",
}

FindType = Literal["movie", "person", "tv", "tv_episode", "tv_season"]
find_types: set[FindType] = {"movie", "person", "tv", "tv_episode", "tv_season"}

FindResult = dict[str, Any]


@backoff.on_exception(
    backoff.constant,
    requests.exceptions.HTTPError,
    interval=30,
    max_tries=3,
)
def find(
    id: str | int,
    source: FindSource,
    type: FindType,
    api_key: str | None = TMDB_API_KEY,
) -> FindResult | None:
    assert source in find_sources
    assert type in find_types

    resp = api_request(
        f"/find/{id}",
        params={"external_source": source},
        api_key=api_key,
    )

    results = resp.get(f"{type}_results")
    if results and len(results) == 1:
        return results[0]
    else:
        return None


ChangeType = Literal["movie", "person", "tv"]
change_types: set[ChangeType] = {"movie", "person", "tv"}


def changes(
    type: ChangeType,
    page: int = 1,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    api_key: str | None = TMDB_API_KEY,
) -> Iterator[int]:
    assert type in change_types
    assert page >= 1
    assert page <= 1000

    params = {
        "page": str(page),
    }

    if start_date:
        params["start_date"] = start_date.isoformat()

    if end_date:
        params["end_date"] = end_date.isoformat()

    resp = api_request(f"/{type}/changes", params=params, api_key=api_key)
    assert resp.get("success", True)

    assert page == resp["page"]

    for result in resp["results"]:
        id: int = result["id"]
        yield id

    total_pages: int = resp["total_pages"]
    if page < total_pages:
        yield from changes(type, page + 1, api_key=api_key)
