# pyright: strict

import datetime

import polars as pl
from polars.testing import assert_series_equal

from polars_requests import request_text, request_urls, response_date


def test_request_urls() -> None:
    urls = pl.Series(
        name="urls",
        values=[
            "http://httpbin.org/get?foo=1",
            "http://httpbin.org/get?foo=2",
            "http://httpbin.org/get?foo=3",
            None,
        ],
    )
    responses = request_urls(urls)
    assert responses.dtype == pl.Object
    assert len(responses) == 4


def test_response_date() -> None:
    urls = pl.Series(
        name="urls",
        values=["http://httpbin.org/get"],
    )
    responses = request_urls(urls)
    dates = response_date(responses)
    assert len(dates) == 1
    assert isinstance(dates[0], datetime.datetime)


def test_request_text() -> None:
    urls = pl.Series(
        name="urls",
        values=[
            "http://httpbin.org/get?foo=1",
            "http://httpbin.org/get?foo=2",
            "http://httpbin.org/get?foo=3",
        ],
    )
    texts = request_text(urls)
    assert texts.dtype == pl.Utf8
    assert len(texts) == 3

    data = texts.str.json_extract()
    args = pl.Series(
        name="args",
        values=[{"foo": "1"}, {"foo": "2"}, {"foo": "3"}],
        dtype=pl.Struct([pl.Field("foo", pl.Utf8)]),
    )
    assert_series_equal(data.struct.field("args"), args)
