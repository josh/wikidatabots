# pyright: strict, reportUnknownMemberType=false, reportUnknownVariableType=false

import sys
from dataclasses import dataclass, field
from functools import partial
from typing import Iterator, TypedDict

import polars as pl
import urllib3
from tqdm import tqdm
from urllib3.exceptions import ResponseError


class _HTTPDict(TypedDict):
    name: str
    value: str


# class _HTTPRequest(TypedDict):
#     url: str
#     fields: list[_HTTPDict]
#     headers: list[_HTTPDict]


class _HTTPResponse(TypedDict):
    status: int
    headers: list[_HTTPDict]
    data: bytes


_HTTP_DICT_DTYPE = pl.Struct([pl.Field("name", pl.Utf8), pl.Field("value", pl.Utf8)])

_HTTP_DICT_SCHEMA = _HTTP_DICT_DTYPE.to_schema()

HTTP_REQUEST_DTYPE = pl.Struct(
    [
        pl.Field("url", pl.Utf8),
        pl.Field("fields", pl.List(_HTTP_DICT_DTYPE)),
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

# _HTTP_RESPONSE_SCHEMA = HTTP_RESPONSE_DTYPE.to_schema()


@dataclass
class Session:
    num_pools: int = 10
    maxsize: int = 1
    block: bool = True

    fields: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)

    connect_timeout: float = 1.0
    read_timeout: float = 10.0

    ok_statuses: set[int] = field(default_factory=lambda: {200})
    retry_statuses: set[int] = field(default_factory=lambda: {413, 429, 503})

    retry_count: int = 0
    retry_allowed_methods: list[str] = field(default_factory=lambda: ["HEAD", "GET"])
    retry_backoff_factor: float = 0.0
    retry_raise_on_redirect: bool = True
    retry_raise_on_status: bool = True
    retry_respect_retry_after_header: bool = True

    def __post_init__(self) -> None:
        timeout = urllib3.Timeout(
            connect=self.connect_timeout,
            read=self.read_timeout,
        )

        retries = urllib3.Retry(
            total=self.retry_count,
            allowed_methods=self.retry_allowed_methods,
            status_forcelist=self.retry_statuses,
            backoff_factor=self.retry_backoff_factor,
            raise_on_redirect=self.retry_raise_on_redirect,
            raise_on_status=self.retry_raise_on_status,
            respect_retry_after_header=self.retry_respect_retry_after_header,
        )

        self._poolmanager = urllib3.PoolManager(
            num_pools=self.num_pools,
            timeout=timeout,
            maxsize=self.maxsize,
            block=self.block,
            headers=self.headers,
            retries=retries,
        )

    def poolmanager(self) -> urllib3.PoolManager:
        return self._poolmanager


def urllib3_requests(requests: pl.Expr, session: Session, log_group: str) -> pl.Expr:
    return requests.map(
        partial(_urllib3_requests_series, session=session, log_group=log_group),
        return_dtype=HTTP_RESPONSE_DTYPE,
    ).alias("response")


def _urllib3_requests_series(
    requests: pl.Series,
    session: Session,
    log_group: str,
) -> pl.Series:
    assert len(requests) < 50_000, f"Too many requests: {len(requests):,}"

    def _values() -> Iterator[_HTTPResponse | None]:
        print(f"::group::{log_group}", file=sys.stderr)

        for request in tqdm(requests, desc="Fetching URLs", unit="url"):
            if request:
                yield _urllib3_request(
                    session=session,
                    url=request["url"],
                    fields=request["fields"],
                    headers=request["headers"],
                )
            else:
                yield None

        print("::endgroup::", file=sys.stderr)

    if len(requests) == 0:
        # FIXME: Polars bug, can't create empty series with dtype
        return pl.Series(name="response").cast(HTTP_RESPONSE_DTYPE)
    else:
        return pl.Series(name="response", values=_values(), dtype=HTTP_RESPONSE_DTYPE)


def _urllib3_request(
    session: Session,
    url: str,
    fields: list[_HTTPDict] | None = None,
    headers: list[_HTTPDict] | None = None,
) -> _HTTPResponse:
    http = session.poolmanager()

    fields_dict = session.fields.copy()
    if fields:
        for f in fields:
            fields_dict[f["name"]] = f["value"]

    headers_dict = session.headers.copy()
    if headers:
        for h in headers:
            headers_dict[h["name"]] = h["value"]

    response: urllib3.HTTPResponse = http.request(
        method="GET",
        url=url,
        fields=fields_dict,
        headers=headers_dict,
    )

    if response.status not in session.ok_statuses:
        raise ResponseError(f"unretryable {response.status} error response")

    resp_headers: list[_HTTPDict] = []
    for name, value in response.headers.items():
        resp_headers.append({"name": name, "value": value})

    return {"status": response.status, "headers": resp_headers, "data": response.data}


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


# FIXME: Patch polars to allow empty lists
_EMPTY_LIST = pl.concat_list([pl.lit(None, dtype=pl.Utf8)]).arr.eval(
    pl.element().drop_nulls()
)


def _http_dict(pairs: dict[str, pl.Expr | str]) -> pl.Expr:
    if len(pairs) == 0:
        return _EMPTY_LIST
    return pl.concat_list([_http_dict_struct(n, v) for n, v in pairs.items()])


def prepare_request(
    url: pl.Expr | str,
    fields: dict[str, pl.Expr | str] = {},
    headers: dict[str, pl.Expr | str] = {},
) -> pl.Expr:
    if isinstance(url, str):
        url = pl.lit(url)

    expr = pl.struct(
        [
            url.alias("url"),
            _http_dict(fields).alias("fields"),
            _http_dict(headers).alias("headers"),
        ],
        schema=_HTTP_REQUEST_SCHEMA,
    ).alias("request")
    assert isinstance(expr, pl.Expr)
    return expr


def response_ok(response: pl.Expr) -> pl.Expr:
    return (
        (response.struct.field("status") >= 200)
        & (response.struct.field("status") < 300)
    ).alias("ok")


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
