# pyright: strict, reportUnknownMemberType=false, reportUnknownVariableType=false

from dataclasses import dataclass, field
from functools import partial
from typing import TypedDict

import polars as pl
import urllib3
from tqdm import tqdm
from urllib3.exceptions import ResponseError


class HTTPDict(TypedDict):
    name: str
    value: str


class HTTPRequest(TypedDict):
    url: str
    fields: list[HTTPDict]
    headers: list[HTTPDict]


class HTTPResponse(TypedDict):
    status: int
    headers: list[HTTPDict]
    data: bytes


HTTP_DICT_DTYPE = pl.Struct([pl.Field("name", pl.Utf8), pl.Field("value", pl.Utf8)])

HTTP_REQUEST_DTYPE = pl.Struct(
    [
        pl.Field("url", pl.Utf8),
        pl.Field("fields", pl.List(HTTP_DICT_DTYPE)),
        pl.Field("headers", pl.List(HTTP_DICT_DTYPE)),
    ]
)

HTTP_RESPONSE_DTYPE = pl.Struct(
    [
        pl.Field("status", pl.UInt16),
        pl.Field("headers", pl.List(HTTP_DICT_DTYPE)),
        pl.Field("data", pl.Binary),
    ]
)


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


def urllib3_requests(requests: pl.Expr, session: Session) -> pl.Expr:
    return requests.map(
        partial(_urllib3_requests_series, session=session),
        return_dtype=HTTP_RESPONSE_DTYPE,
    ).alias("response")


def _urllib3_requests_series(requests: pl.Series, session: Session) -> pl.Series:
    def values():
        for request in tqdm(requests, desc="Fetching URLs", unit="row"):
            if request:
                r: HTTPRequest = request

                url = r["url"]

                fields: dict[str, str] = {}
                for f in r["fields"]:
                    fields[f["name"]] = f["value"]

                headers: dict[str, str] = {}
                for h in r["headers"]:
                    headers[h["name"]] = h["value"]

                yield _urllib3_request(
                    session=session,
                    url=url,
                    fields=fields,
                    headers=headers,
                )
            else:
                yield None

    return pl.Series("response", values(), dtype=HTTP_RESPONSE_DTYPE)


def urllib3_request_urls(urls: pl.Expr, session: Session) -> pl.Expr:
    return urls.map(
        partial(_urllib3_request_urls_series, session=session),
        return_dtype=HTTP_RESPONSE_DTYPE,
    ).alias("response")


def _urllib3_request_urls_series(urls: pl.Series, session: Session) -> pl.Series:
    def values():
        for url in tqdm(urls, desc="Fetching URLs", unit="row"):
            if url:
                yield _urllib3_request(session=session, url=url)
            else:
                yield None

    return pl.Series("response", values(), dtype=HTTP_RESPONSE_DTYPE)


def _urllib3_request(
    session: Session,
    url: str,
    fields: dict[str, str] = {},
    headers: dict[str, str] = {},
) -> HTTPResponse:
    http = session.poolmanager()

    fields = dict(session.fields, **fields)
    headers = dict(session.headers, **headers)

    response: urllib3.HTTPResponse = http.request(
        method="GET",
        url=url,
        fields=fields,
        headers=headers,
    )

    if response.status not in session.ok_statuses:
        raise ResponseError(f"unretryable {response.status} error response")

    resp_headers: list[HTTPDict] = []
    for name, value in response.headers.items():
        resp_headers.append({"name": name, "value": value})

    return {"status": response.status, "headers": resp_headers, "data": response.data}


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
