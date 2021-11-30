import backoff
import requests

from utils import batches


@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_tries=3)
def lookup(ids, country=None, session=requests.Session()):
    params = {}

    if type(ids) == list:
        params["id"] = ",".join(map(str, ids))
    else:
        params["id"] = str(ids)

    if country:
        params["country"] = country

    url = "https://itunes.apple.com/lookup"
    r = session.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    return data["results"]


def batch_lookup(ids, country="us"):
    """
    Look up many iTunes tracks by IDs.
    """

    session = requests.Session()

    for ids_batch in batches(ids, size=150):
        results = {}

        for result in lookup(ids_batch, country=country, session=session):
            type = result["wrapperType"]
            id = result.get(type + "Id") or result["trackId"]
            results[id] = result

        for id in ids_batch:
            yield (id, results.get(id))


countries = ["us", "gb", "au", "br", "de", "ca", "it", "es", "fr", "jp", "jp", "cn"]


def all_not_found(id):
    for country in countries:
        (id2, found) = next(batch_lookup([id], country=country))
        assert id == id2
        if found:
            return False
    return True
