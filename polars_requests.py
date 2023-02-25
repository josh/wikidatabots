# pyright: strict

from typing import Iterator

import polars as pl
import requests
from tqdm import tqdm


def request_urls(urls: pl.Series) -> pl.Series:
    def values() -> Iterator[requests.Response | None]:
        with requests.Session() as s:
            for url in tqdm(urls, desc="Fetching URLs", unit="url"):
                if url:
                    yield s.get(url)
                else:
                    yield None

    return pl.Series(name="response", values=values(), dtype=pl.Object)


def response_status_code(responses: pl.Series) -> pl.Series:
    return responses.apply(_response_status_code, return_dtype=pl.UInt16)


def response_date(responses: pl.Series) -> pl.Series:
    return responses.apply(_response_headers_date, return_dtype=pl.Utf8).str.strptime(
        pl.Datetime(time_unit="ms"), "%a, %d %b %Y %H:%M:%S %Z", strict=True
    )


def response_text(responses: pl.Series) -> pl.Series:
    return responses.apply(_response_text, return_dtype=pl.Utf8)


def response_content(responses: pl.Series) -> pl.Series:
    return responses.apply(_response_content, return_dtype=pl.Binary)


def request_text(urls: pl.Series) -> pl.Series:
    return response_text(request_urls(urls))


def _response_status_code(response: requests.Response) -> int:
    return response.status_code


def _response_headers_date(response: requests.Response) -> str | None:
    return response.headers.get("Date")


def _response_text(response: requests.Response) -> str:
    return response.text


def _response_content(response: requests.Response) -> bytes:
    return response.content
