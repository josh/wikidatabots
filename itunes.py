# pyright: strict

from collections.abc import Iterable, Iterator
from typing import Literal, TypedDict

import backoff
import polars as pl
import requests
from tqdm import tqdm

from utils import batches

ID = int

session: requests.Session = requests.Session()

Country = Literal[
    "us",
    "gb",
    "au",
    "br",
    "de",
    "ca",
    "it",
    "es",
    "fr",
    "jp",
    "jp",
    "cn",
]

COUNTRIES: set[Country] = {
    "us",
    "gb",
    "au",
    "br",
    "de",
    "ca",
    "it",
    "es",
    "fr",
    "jp",
    "jp",
    "cn",
}


class LookupResult(TypedDict):
    wrapperType: str
    trackId: int
    trackName: str


@backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_tries=5)
def lookup(ids: list[ID] | ID, country: Country | None = None) -> list[LookupResult]:
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


MAX_BATCH_LOOKUP_SIZE = 150


def batch_lookup(
    ids: Iterable[ID],
    country: Country = "us",
) -> Iterator[tuple[ID, LookupResult | None]]:
    """
    Look up many iTunes tracks by IDs.
    """

    for ids_batch in batches(ids, size=MAX_BATCH_LOOKUP_SIZE):
        results: dict[int, LookupResult] = {}

        for result in lookup(ids_batch, country=country):
            type = result["wrapperType"]
            id: int = result.get(f"{type}Id") or result["trackId"]
            results[id] = result

        for id in ids_batch:
            yield (id, results.get(id))


def _id_series_ok(ids: pl.Series, country: Country) -> pl.Series:
    def _values():
        for _, obj in tqdm(
            batch_lookup(ids, country=country),
            desc="itunes_lookup",
            total=len(ids),
        ):
            if obj:
                yield True
            else:
                yield False

    return pl.Series(name=ids.name, values=_values(), dtype=pl.Boolean)


def id_expr_ok(ids: pl.Expr, country: Country) -> pl.Expr:
    def _inner(s: pl.Series) -> pl.Series:
        return _id_series_ok(s, country=country)

    return ids.map(_inner, return_dtype=pl.Boolean).alias(f"country_{country}")
