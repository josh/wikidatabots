# pyright: strict, reportUnknownMemberType=false, reportUnknownVariableType=false

from dataclasses import dataclass, field
from functools import partial
from typing import Iterable, TypedDict

import polars as pl
import urllib3
from tqdm import tqdm
from urllib3.exceptions import ResponseError

from actions import log_group as _log_group
from polars_utils import apply_with_tqdm


class _HTTPDict(TypedDict):
    name: str
    value: str


class _HTTPResponse(TypedDict):
    status: int
    headers: list[_HTTPDict]
    data: bytes


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


@dataclass
class Session:
    num_pools: int = 10
    maxsize: int = 1
    block: bool = True

    connect_timeout: float = 1.0
    read_timeout: float = 10.0

    ok_statuses: set[int] = field(default_factory=lambda: {200})
    retry_statuses: set[int] = field(default_factory=lambda: {413, 429, 503})

    retry_count: int = 0
    retry_allowed_methods: list[str] = field(default_factory=lambda: ["HEAD", "GET"])
    retry_backoff_factor: float = 0.0
    retry_raise_on_redirect: bool = True
    retry_raise_on_status: bool = True

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
            respect_retry_after_header=True,
        )

        self._poolmanager = urllib3.PoolManager(
            num_pools=self.num_pools,
            timeout=timeout,
            maxsize=self.maxsize,
            block=self.block,
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

    if len(requests) == 0:
        return pl.Series(name="response", values=[], dtype=HTTP_RESPONSE_DTYPE)

    values: list[_HTTPResponse | None] = []

    with _log_group(log_group):
        for request in tqdm(requests, unit="url"):
            if request and request["url"]:
                response = _urllib3_request(
                    session=session,
                    url=request["url"],
                    headers=request["headers"],
                )
                values.append(response)
            else:
                values.append(None)

    return pl.Series(name="response", values=values, dtype=HTTP_RESPONSE_DTYPE)


def _urllib3_request(
    session: Session,
    url: str,
    headers: list[_HTTPDict] | None = None,
) -> _HTTPResponse:
    http = session.poolmanager()

    headers_dict = {}
    if headers:
        for h in headers:
            headers_dict[h["name"]] = h["value"]

    response: urllib3.HTTPResponse = http.request(
        method="GET",
        url=url,
        headers=headers_dict,
        redirect=False,
    )

    if response.status not in session.ok_statuses:
        raise ResponseError(f"unretryable {response.status} error response")

    resp_headers: list[_HTTPDict] = []
    header_items: Iterable[tuple[str, str]] = response.headers.items()
    for name, value in header_items:
        resp_headers.append({"name": name, "value": value})

    return {"status": response.status, "headers": resp_headers, "data": response.data}


def urllib3_resolve_redirects(
    url: pl.Expr,
    session: Session,
    log_group: str,
) -> pl.Expr:
    def _resolve_redirect(url: str) -> str:
        response: urllib3.HTTPResponse = session.poolmanager().request(
            method="HEAD",
            url=url,
            redirect=True,
        )
        return response.geturl()  # type: ignore

    return url.pipe(
        apply_with_tqdm,
        _resolve_redirect,
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
