# pyright: strict

import logging
import time
from functools import partial
from typing import Callable, Iterable, ParamSpec, TypedDict, TypeVar

import backoff
import polars as pl
import requests as _requests
from tqdm import tqdm

from actions import log_group as _log_group
from polars_utils import apply_with_tqdm


class _HTTPDict(TypedDict):
    name: str
    value: str


def _make_header_dict(headers: list[_HTTPDict] | None) -> dict[str, str]:
    if not headers:
        return {}
    return {h["name"]: h["value"] for h in headers}


def _make_header_list(headers: dict[str, str]) -> list[_HTTPDict]:
    return [{"name": name, "value": value} for name, value in headers.items()]


class _HTTPRequest(TypedDict):
    url: str
    headers: list[_HTTPDict] | None


class _HTTPResponse(TypedDict):
    status: int
    headers: list[_HTTPDict]
    data: bytes


def _make_http_response(response: _requests.Response) -> _HTTPResponse:
    return {
        "status": response.status_code,
        "headers": _make_header_list(dict(response.headers)),
        "data": response.content,
    }


_HTTP_DICT_DTYPE = pl.Struct([pl.Field("name", pl.Utf8), pl.Field("value", pl.Utf8)])

_HTTP_DICT_SCHEMA = _HTTP_DICT_DTYPE.to_schema()

HTTP_REQUEST_DTYPE = pl.Struct(
    [
        pl.Field("url", pl.Utf8),
        pl.Field("headers", pl.List(_HTTP_DICT_DTYPE)),
    ]
)

_HTTP_REQUEST_SCHEMA = HTTP_REQUEST_DTYPE.to_schema()

HTTP_RESPONSE_DTYPE = pl.Struct(
    [
        pl.Field("status", pl.UInt16),
        pl.Field("headers", pl.List(_HTTP_DICT_DTYPE)),
        pl.Field("data", pl.Binary),
    ]
)


def request(
    requests: pl.Expr,
    log_group: str,
    timeout: float = 10.0,
    min_time: float = 0.0,
    ok_statuses: Iterable[int] = [200],
    bad_statuses: Iterable[int] = [],
    retry_count: int = 0,
) -> pl.Expr:
    # MARK: pl.Expr.map
    return requests.map(
        partial(
            _request_series,
            log_group=log_group,
            timeout=timeout,
            min_time=min_time,
            ok_statuses=ok_statuses,
            bad_statuses=bad_statuses,
            retry_count=retry_count,
        ),
        return_dtype=HTTP_RESPONSE_DTYPE,
    ).alias("response")


def _request_series(
    requests: pl.Series,
    log_group: str,
    timeout: float,
    min_time: float,
    ok_statuses: Iterable[int],
    bad_statuses: Iterable[int],
    retry_count: int,
) -> pl.Series:
    assert len(requests) < 50_000, f"Too many requests: {len(requests):,}"

    if len(requests) == 0:
        return pl.Series(name="response", values=[], dtype=HTTP_RESPONSE_DTYPE)

    session = _requests.Session()
    ok_status_codes = set(ok_statuses)
    bad_status_codes = set(bad_statuses)
    disable_tqdm = len(requests) <= 1

    response_codes: list[int | None] = [None] * len(requests)
    elapsed_times: list[float | None] = [None] * len(requests)

    def request_with_retry(
        request_id: int,
        url: str,
        headers: dict[str, str],
    ) -> _requests.Response:
        start_time = time.time()
        r = session.request(
            method="GET",
            url=url,
            headers=headers,
            timeout=timeout,
            allow_redirects=False,
        )

        previous_status_code = response_codes[request_id]
        response_codes[request_id] = r.status_code
        if previous_status_code and previous_status_code != r.status_code:
            if r.status_code in bad_status_codes:
                logging.debug(f"Retried {previous_status_code} -> {r.status_code}")
            else:
                logging.warning(f"Retried {previous_status_code} -> {r.status_code}")

        elapsed_time = time.time() - start_time
        elapsed_times[request_id] = elapsed_time

        if r.status_code in ok_status_codes:
            pass
        elif r.status_code in bad_status_codes:
            r.raise_for_status()
        else:
            logging.warning(f"Unknown status code: {r.status_code}")
            r.raise_for_status()

        sleep_time = min_time - elapsed_time
        if sleep_time > 0:
            logging.debug(f"Sleeping for {sleep_time:.2f} seconds more seconds")
            time.sleep(sleep_time)

        return r

    request_with_retry = _decorate_backoff(request_with_retry, retry_count)

    values: list[_HTTPResponse | None] = []
    with _log_group(log_group):
        request_id = 0
        for request_ in tqdm(requests, unit="url", disable=disable_tqdm):
            request: _HTTPRequest = request_
            response = None
            if request and request["url"]:
                r = request_with_retry(
                    request_id,
                    request["url"],
                    _make_header_dict(request["headers"]),
                )
                response = _make_http_response(r)
            request_id += 1
            values.append(response)

        elapsed_times_s = pl.Series(elapsed_times)
        logging.info("Elapsed times: %s", elapsed_times_s.describe())
        session.close()

    return pl.Series(name="response", values=values, dtype=HTTP_RESPONSE_DTYPE)


T = TypeVar("T")
P = ParamSpec("P")


def _decorate_backoff(fn: Callable[P, T], max_retries: int) -> Callable[P, T]:
    assert max_retries <= 12, "Too many retries"
    if max_retries:
        return backoff.on_exception(
            backoff.expo,
            _requests.exceptions.RequestException,
            max_tries=max_retries,
        )(fn)
    else:
        return fn


def resolve_redirects(
    url: pl.Expr,
    log_group: str,
    timeout: float = 10.0,
    ok_statuses: Iterable[int] = [],
    retry_count: int = 0,
) -> pl.Expr:
    session = _requests.Session()
    ok_status_codes = set(ok_statuses)

    def resolve_redirect(url: str) -> str:
        r = session.request(
            method="HEAD",
            url=url,
            timeout=timeout,
            allow_redirects=True,
        )

        if r.status_code not in ok_status_codes:
            r.raise_for_status()

        return r.url

    resolve_redirect = _decorate_backoff(resolve_redirect, retry_count)

    return url.pipe(
        apply_with_tqdm,
        resolve_redirect,
        return_dtype=pl.Utf8,
        log_group=log_group,
    )


def _http_dict_struct(name: str, value: pl.Expr | str) -> pl.Expr:
    if isinstance(value, str):
        value = pl.lit(value)

    expr = pl.struct(
        [
            pl.lit(name).alias("name"),
            value.alias("value"),
        ],
        schema=_HTTP_DICT_SCHEMA,
    )
    assert isinstance(expr, pl.Expr)
    return expr


def _http_dict(pairs: dict[str, pl.Expr | str]) -> pl.Expr | pl.Series:
    if len(pairs) == 0:
        return pl.Series(values=[None], dtype=pl.List(_HTTP_DICT_DTYPE))
    return pl.concat_list([_http_dict_struct(n, v) for n, v in pairs.items()])


def _wrap_lit_expr(value: str | pl.Expr) -> pl.Expr:
    if isinstance(value, str):
        return pl.lit(value)
    return value


def prepare_request(
    url: pl.Expr | str,
    fields: dict[str, pl.Expr | str] = {},
    headers: dict[str, pl.Expr | str] = {},
) -> pl.Expr:
    url = _wrap_lit_expr(url)

    if fields:
        f_string = "{}?" + "&".join(name + "={}" for name in fields)
        field_values = [_wrap_lit_expr(v) for v in fields.values()]
        url = pl.format(f_string, url, *field_values)

    expr = pl.struct(
        [
            url.alias("url"),
            _http_dict(headers).alias("headers"),
        ],
        schema=_HTTP_REQUEST_SCHEMA,
    ).alias("request")
    assert isinstance(expr, pl.Expr)
    return expr


def response_header_value(response: pl.Expr, name: str) -> pl.Expr:
    return (
        response.struct.field("headers")
        .arr.eval(
            pl.element()
            .where(pl.element().struct.field("name") == name)
            .struct.field("value")
        )
        .arr.first()
        .alias(name)
    )


def response_date(response: pl.Expr) -> pl.Expr:
    return (
        response.pipe(response_header_value, name="Date")
        .str.strptime(
            pl.Datetime(time_unit="ms"), "%a, %d %b %Y %H:%M:%S %Z", strict=True
        )
        .alias("response_date")
    )


def response_text(response: pl.Expr) -> pl.Expr:
    return response.struct.field("data").cast(pl.Utf8).alias("response_text")
