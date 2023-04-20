# pyright: strict

from collections.abc import Iterable, Iterator
from typing import TypedDict

import requests

_session: requests.Session = requests.Session()


class _LookupResult(TypedDict):
    wrapperType: str
    trackId: int
    trackName: str


def batch_lookup(ids: Iterable[int]) -> Iterator[tuple[int, _LookupResult | None]]:
    for ids_batch in _batches(ids, size=150):
        results: dict[int, _LookupResult] = {}

        for result in _lookup(ids_batch):
            type = result["wrapperType"]
            id: int = result.get(f"{type}Id") or result["trackId"]
            results[id] = result

        for id in ids_batch:
            yield (id, results.get(id))


def _lookup(ids: list[int]) -> list[_LookupResult]:
    params: dict[str, str] = {
        "id": ",".join(map(str, ids)),
        "country": "us",
    }

    url = "https://itunes.apple.com/lookup"
    r = _session.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    return data["results"]


def _batches(iterable: Iterable[int], size: int) -> Iterator[list[int]]:
    batch: list[int] = []

    for element in iterable:
        batch.append(element)
        if len(batch) == size:
            yield batch
            batch = []

    if batch:
        yield batch
