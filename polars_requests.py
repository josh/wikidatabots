import time
from collections.abc import Callable, Iterable
from functools import partial
from typing import ParamSpec, TypedDict, TypeVar

import backoff
import polars as pl
import requests as _requests
from tqdm import tqdm

from actions import log_group as _log_group
from actions import warn


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
_HTTP_DICT_SCHEMA = dict(_HTTP_DICT_DTYPE)

HTTP_REQUEST_DTYPE = pl.Struct(
    [
        pl.Field("url", pl.Utf8),
        pl.Field("headers", pl.List(_HTTP_DICT_DTYPE)),
    ]
)
_HTTP_REQUEST_SCHEMA = dict(HTTP_REQUEST_DTYPE)

HTTP_RESPONSE_DTYPE = pl.Struct(
    [
        pl.Field("status", pl.UInt16),
        pl.Field("headers", pl.List(_HTTP_DICT_DTYPE)),
        pl.Field("data", pl.Binary),
    ]
)


class StatusCodeWarning(Warning):
    pass


T = TypeVar("T")
P = ParamSpec("P")


def _decorate_backoff[**P, T](fn: Callable[P, T], max_retries: int) -> Callable[P, T]:
    assert max_retries <= 12, "Too many retries"
    if max_retries:
        return backoff.on_exception(
            backoff.expo,
            _requests.exceptions.RequestException,
            max_tries=max_retries,
            max_time=300,
        )(fn)
    else:
        return fn


def _request_series(
    requests: pl.Series,
    log_group: str,
    timeout: float,
    min_time: float,
    ok_statuses: set[int],
    bad_statuses: set[int],
    retry_count: int,
) -> pl.Series:
    assert len(requests) < 50_000, f"Too many requests: {len(requests):,}"

    if len(requests) == 0:
        return pl.Series(name="response", values=[], dtype=HTTP_RESPONSE_DTYPE)

    session = _requests.Session()
    disable_tqdm = len(requests) <= 1

    response_codes: list[int | None] = [None] * len(requests)

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
            if previous_status_code in bad_statuses:
                pass
            else:
                warn(
                    f"Retried {previous_status_code} -> {r.status_code}",
                    StatusCodeWarning,
                )

        elapsed_time = time.time() - start_time

        if r.status_code in ok_statuses:
            pass
        elif r.status_code in bad_statuses:
            r.raise_for_status()
        else:
            warn(f"Unknown status code: {r.status_code}", StatusCodeWarning)
            r.raise_for_status()

        sleep_time = min_time - elapsed_time
        if sleep_time > 0:
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

        session.close()

    return pl.Series(name="response", values=values, dtype=HTTP_RESPONSE_DTYPE)


def request(
    requests: pl.Expr,
    log_group: str,
    timeout: float = 10.0,
    min_time: float = 0.0,
    ok_statuses: Iterable[int] = [200],
    bad_statuses: Iterable[int] = [],
    retry_count: int = 0,
) -> pl.Expr:
    # MARK: pl.Expr.map_batches
    return requests.map_batches(
        partial(
            _request_series,
            log_group=log_group,
            timeout=timeout,
            min_time=min_time,
            ok_statuses=set(ok_statuses),
            bad_statuses=set(bad_statuses),
            retry_count=retry_count,
        ),
        return_dtype=HTTP_RESPONSE_DTYPE,
    ).alias("response")


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


def response_text(response: pl.Expr) -> pl.Expr:
    return response.struct.field("data").cast(pl.Utf8).alias("response_text")
