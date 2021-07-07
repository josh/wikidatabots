import requests

from utils import batches


def batch_lookup(ids, country="us"):
    """
    Look up many iTunes tracks by IDs.
    """

    session = requests.Session()

    for ids_batch in batches(ids, size=150):
        ids_str = ",".join(map(str, ids_batch))
        params = {"country": country, "id": ids_str}
        r = session.get("https://itunes.apple.com/lookup", params=params)
        r.raise_for_status()
        json = r.json()
        results = {}

        for result in json["results"]:
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
