# pyright: strict

import datetime

import polars as pl
from polars.testing import assert_series_equal

from polars_requests import (
    Session,
    request_url_expr,
    request_url_expr_text,
    request_url_series,
    request_url_series_text,
    response_expr_text,
    response_series_date,
)

_HTTPBIN_SESSION = Session(host="httpbin.org", connect_timeout=0.1, read_timeout=2.0)


def test_request_url_expr() -> None:
    df = pl.DataFrame(
        {
            "url": [
                "https://httpbin.org/get?foo=1",
                "https://httpbin.org/get?foo=2",
                "https://httpbin.org/get?foo=3",
            ]
        }
    ).with_columns(
        request_url_expr(pl.col("url"), session=_HTTPBIN_SESSION).alias("response"),
    )
    assert df.shape == (3, 2)
    assert df.schema == {"url": pl.Utf8, "response": pl.Object}


def test_request_url_expr_text() -> None:
    df = pl.DataFrame(
        {
            "url": [
                "https://httpbin.org/get?foo=1",
                "https://httpbin.org/get?foo=2",
                "https://httpbin.org/get?foo=3",
            ]
        }
    ).with_columns(
        request_url_expr_text(pl.col("url"), session=_HTTPBIN_SESSION).alias(
            "response_text"
        ),
    )
    assert df.shape == (3, 2)
    assert df.schema == {"url": pl.Utf8, "response_text": pl.Utf8}


def test_request_url_series() -> None:
    urls = pl.Series(
        name="urls",
        values=[
            "https://httpbin.org/get?foo=1",
            "https://httpbin.org/get?foo=2",
            "https://httpbin.org/get?foo=3",
            None,
        ],
    )
    responses = request_url_series(urls, session=_HTTPBIN_SESSION)
    assert responses.dtype == pl.Object
    assert len(responses) == 4


def test_request_url_series_text() -> None:
    urls = pl.Series(
        name="url",
        values=[
            "https://httpbin.org/get?foo=1",
            "https://httpbin.org/get?foo=2",
            "https://httpbin.org/get?foo=3",
        ],
    )
    texts = request_url_series_text(urls, session=_HTTPBIN_SESSION)
    assert texts.dtype == pl.Utf8
    assert len(texts) == 3

    data = texts.str.json_extract()
    args = pl.Series(
        name="args",
        values=[{"foo": "1"}, {"foo": "2"}, {"foo": "3"}],
        dtype=pl.Struct([pl.Field("foo", pl.Utf8)]),
    )
    assert_series_equal(data.struct.field("args"), args)


def test_response_expr_text() -> None:
    df = (
        pl.DataFrame({"url": ["https://httpbin.org/get"]})
        .with_columns(
            request_url_expr(pl.col("url"), session=_HTTPBIN_SESSION).alias("response"),
        )
        .with_columns(
            response_expr_text(pl.col("response")).alias("response_text"),
        )
    )
    assert df.shape == (1, 3)
    assert df.schema == {
        "url": pl.Utf8,
        "response": pl.Object,
        "response_text": pl.Utf8,
    }


def test_response_series_date() -> None:
    urls = pl.Series(
        name="urls",
        values=["https://httpbin.org/get"],
    )
    responses = request_url_series(urls, session=_HTTPBIN_SESSION)
    dates = response_series_date(responses)
    assert len(dates) == 1
    assert isinstance(dates[0], datetime.datetime)
