import os

import backoff
import requests

TMDB_API_KEY = os.environ.get("TMDB_API_KEY")


class UnauthorizedException(Exception):
    pass


@backoff.on_exception(backoff.expo, requests.exceptions.ConnectionError, max_tries=3)
def api_request(path, params={}, version=3, api_key=TMDB_API_KEY):
    url = "https://api.themoviedb.org/{}{}".format(str(version), path)
    post_params = {}
    if api_key:
        post_params["api_key"] = api_key
    post_params.update(params)
    r = requests.get(url, params=post_params)
    if r.headers["Content-Type"].startswith("application/json"):
        data = r.json()
        if r.status_code == 401:
            raise UnauthorizedException(data["status_message"])
        return data
    else:
        r.raise_for_status()
        return {}


object_types = set(["movie", "tv", "person"])


def object(id, type, append=[], api_key=TMDB_API_KEY):
    assert type in object_types

    params = {}
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


find_sources = set(
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

find_types = set(["movie", "person", "tv", "tv_episode", "tv_season"])


def find(id, source, type, api_key=TMDB_API_KEY):
    assert source in find_sources
    assert type in find_types

    resp = api_request(
        "/find/{}".format(id),
        params={"external_source": source},
        api_key=api_key,
    )

    results = resp.get("{}_results".format(type))
    if len(results) == 1:
        return results[0]
    else:
        return None
