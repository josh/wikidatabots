from collections.abc import Iterable, Iterator
from typing import Optional, TypedDict

import backoff
import requests

from utils import batches

ID = int

session: requests.Session = requests.Session()


class LookupResult(TypedDict):
    wrapperType: str
    trackId: int
    trackName: str


@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_tries=5)
def lookup(ids: list[ID] | ID, country: Optional[str] = None) -> list[LookupResult]:
    params: dict[str, str] = {}

    if type(ids) is list:
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


def batch_lookup(
    ids: Iterable[ID],
    country: str = "us",
) -> Iterator[tuple[ID, Optional[LookupResult]]]:
    """
    Look up many iTunes tracks by IDs.
    """

    for ids_batch in batches(ids, size=150):
        results: dict[int, LookupResult] = {}

        for result in lookup(ids_batch, country=country):
            type = result["wrapperType"]
            id: int = result.get(type + "Id") or result["trackId"]
            results[id] = result

        for id in ids_batch:
            yield (id, results.get(id))


countries = ["us", "gb", "au", "br", "de", "ca", "it", "es", "fr", "jp", "jp", "cn"]


def all_not_found(id: int) -> bool:
    for country in countries:
        (id2, found) = next(batch_lookup([id], country=country))
        assert id == id2
        if found:
            return False
    return True
