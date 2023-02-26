# pyright: strict

from dataclasses import dataclass, field
from functools import partial
from typing import Iterator, Type

import backoff
import polars as pl
import requests
from tqdm import tqdm


@dataclass
class Session:
    host: str = "*"
    session: requests.Session = field(default_factory=requests.Session)
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, str] = field(default_factory=dict)

    connect_timeout: float = 1.0
    read_timeout: float = 10.0

    ok_status_codes: set[int] = field(default_factory=set)
    retry_exceptions: list[Type[Exception]] = field(default_factory=list)
    retry_max_tries: int = 0
    retry_max_time: float = 3600.0


def request_url_expr(urls: pl.Expr, session: Session = Session()) -> pl.Expr:
    return urls.map(
        partial(request_url_series, session=session), return_dtype=pl.Object
    )


def request_url_expr_text(urls: pl.Expr, session: Session = Session()) -> pl.Expr:
    return urls.map(
        partial(request_url_series_text, session=session), return_dtype=pl.Utf8
    )


def request_url_ldf(url: str, session: Session = Session()) -> pl.LazyFrame:
    return pl.LazyFrame({"url": [url]}).map(
        partial(request_urls_df, session=session), schema={"response": pl.Object}
    )


def request_url_series(
    urls: pl.Series,
    session: Session,
) -> pl.Series:
    def values() -> Iterator[requests.Response | None]:
        for url in tqdm(urls, desc="Fetching URLs", unit="url"):
            if url:
                yield session_get(session, url)
            else:
                yield None

    assert urls.dtype == pl.Utf8
    return pl.Series(name="response", values=values(), dtype=pl.Object)


def request_url_series_text(urls: pl.Series, session: Session) -> pl.Series:
    assert urls.dtype == pl.Utf8
    return response_series_text(request_url_series(urls, session=session))


def request_urls_df(df: pl.DataFrame, session: Session = Session()) -> pl.DataFrame:
    return pl.DataFrame({"response": request_url_series(df["url"], session=session)})


def response_expr_content(responses: pl.Expr) -> pl.Expr:
    return responses.map(response_series_content, return_dtype=pl.Binary)


def response_series_content(responses: pl.Series) -> pl.Series:
    assert responses.dtype == pl.Object
    return responses.apply(_response_content, return_dtype=pl.Binary)


def response_expr_date(responses: pl.Expr) -> pl.Expr:
    return responses.map(response_series_date, return_dtype=pl.Datetime(time_unit="ms"))


def response_series_date(responses: pl.Series) -> pl.Series:
    assert responses.dtype == pl.Object
    return responses.apply(_response_headers_date, return_dtype=pl.Utf8).str.strptime(
        pl.Datetime(time_unit="ms"), "%a, %d %b %Y %H:%M:%S %Z", strict=True
    )


def response_expr_status_code(responses: pl.Expr) -> pl.Expr:
    return responses.map(response_series_status_code, return_dtype=pl.UInt16)


def response_series_status_code(responses: pl.Series) -> pl.Series:
    assert responses.dtype == pl.Object
    return responses.apply(_response_status_code, return_dtype=pl.UInt16)


def response_expr_text(responses: pl.Expr) -> pl.Expr:
    return responses.map(response_series_text, return_dtype=pl.Utf8)


def response_series_text(responses: pl.Series) -> pl.Series:
    assert responses.dtype == pl.Object
    return responses.apply(_response_text, return_dtype=pl.Utf8)


def session_get(session: Session, url: str) -> requests.Response:
    if session.retry_exceptions:
        return backoff.on_exception(
            wait_gen=backoff.expo,
            exception=session.retry_exceptions,
            max_tries=session.retry_max_tries,
            max_time=session.retry_max_time,
        )(_session_get_without_retry)(session, url)
    else:
        return _session_get_without_retry(session, url)


def _response_content(response: requests.Response) -> bytes:
    return response.content


def _response_headers_date(response: requests.Response) -> str | None:
    return response.headers.get("Date")


def _response_status_code(response: requests.Response) -> int:
    return response.status_code


def _response_text(response: requests.Response) -> str:
    return response.text


def _session_get_without_retry(session: Session, url: str) -> requests.Response:
    assert session.host == "*" or url.startswith(
        f"https://{session.host}"
    ), f"Session host ({session.host}) does not match URL: {url}"
    r = session.session.get(
        url=url,
        params=session.params,
        headers=session.headers,
        timeout=(session.connect_timeout, session.read_timeout),
    )
    if r.status_code not in session.ok_status_codes:
        r.raise_for_status()
    return r
