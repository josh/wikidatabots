import os

import requests

TMDB_API_KEY = os.environ.get("TMDB_API_KEY")


class UnauthorizedException(Exception):
    pass


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


def movie(id, api_key=TMDB_API_KEY):
    resp = api_request(
        "/movie/{}".format(id),
        api_key=api_key,
    )
    if resp.get("success") is False:
        return None
    return resp


def person(id, api_key=TMDB_API_KEY):
    resp = api_request(
        "/person/{}".format(id),
        api_key=api_key,
    )
    if resp.get("success") is False:
        return None
    return resp


def find(external_id, external_source, api_key=TMDB_API_KEY):
    return api_request(
        "/find/{}".format(external_id),
        params={"external_source": external_source},
        api_key=api_key,
    )


def find_by_imdb_id(imdb_id, type, api_key=TMDB_API_KEY):
    resp = find(external_id=imdb_id, external_source="imdb_id", api_key=api_key)
    results = resp.get("{}_results".format(type))
    if not results:
        return None
    return str(results[0]["id"])
