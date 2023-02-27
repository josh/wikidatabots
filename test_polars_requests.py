# pyright: strict

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from polars_requests import (
    HTTP_RESPONSE_DTYPE,
    Session,
    prepare_request,
    response_date,
    response_header_value,
    response_ok,
    response_text,
    urllib3_request_urls,
    urllib3_requests,
)

_HTTPBIN_SESSION = Session(connect_timeout=1.0, read_timeout=2.0)


def test_urllib3_request_urls() -> None:
    response_dtype = pl.Struct({"args": pl.Struct({"foo": pl.Utf8})})
    ldf = (
        pl.LazyFrame(
            {
                "url": [
                    "https://httpbin.org/get?foo=1",
                    "https://httpbin.org/get?foo=2",
                    "https://httpbin.org/get?foo=3",
                ]
            }
        )
        .with_columns(
            pl.col("url").pipe(urllib3_request_urls, session=_HTTPBIN_SESSION),
        )
        .with_columns(
            pl.col("response").pipe(response_ok),
            pl.col("response").pipe(response_date).alias("date"),
            (
                pl.col("response")
                .pipe(response_header_value, name="Content-Type")
                .alias("content_type")
            ),
            pl.col("response").pipe(response_text),
        )
        .with_columns(
            pl.col("response_text")
            .str.json_extract(response_dtype)
            .struct.field("args")
            .struct.field("foo")
            .alias("foo"),
        )
    )

    assert ldf.schema == {
        "url": pl.Utf8,
        "response": HTTP_RESPONSE_DTYPE,
        "ok": pl.Boolean,
        "date": pl.Datetime(time_unit="ms"),
        "content_type": pl.Utf8,
        "response_text": pl.Utf8,
        "foo": pl.Utf8,
    }

    df = ldf.collect()
    assert len(df) == 3


def test_urllib3_request_urls_empty() -> None:
    ldf = pl.LazyFrame({"url": []}, schema={"url": pl.Utf8}).with_columns(
        pl.col("url").pipe(urllib3_request_urls, session=_HTTPBIN_SESSION),
    )
    ldf2 = pl.LazyFrame({"url": [], "response": []}).with_columns(
        pl.col("url").cast(pl.Utf8),
        pl.col("response").cast(HTTP_RESPONSE_DTYPE),
    )
    assert_frame_equal(ldf, ldf2)


def test_urllib3_request_urls_with_defaults() -> None:
    session = Session(fields={"foo": "bar"}, headers={"X-Foo": "baz"})
    response_dtype = pl.Struct(
        {
            "args": pl.Struct({"foo": pl.Utf8}),
            "headers": pl.Struct({"X-Foo": pl.Utf8}),
        }
    )
    ldf = pl.LazyFrame({"url": ["https://httpbin.org/get"]}).with_columns(
        pl.col("url")
        .pipe(urllib3_request_urls, session=session)
        .pipe(response_text)
        .str.json_extract(response_dtype)
        .alias("data"),
    )

    ldf2 = pl.LazyFrame(
        {
            "url": ["https://httpbin.org/get"],
            "data": [{"args": {"foo": "bar"}, "headers": {"X-Foo": "baz"}}],
        }
    )

    assert_frame_equal(ldf, ldf2)


def test_urllib3_requests_raw() -> None:
    response_dtype = pl.Struct(
        {
            "args": pl.Struct({"foo": pl.Utf8}),
            "headers": pl.Struct({"X-Foo": pl.Utf8}),
        }
    )

    requests = pl.Series(
        [
            {
                "url": "https://httpbin.org/get",
                "fields": [{"name": "foo", "value": "bar"}],
                "headers": [{"name": "X-Foo", "value": "baz"}],
            }
        ],
    )

    ldf = pl.LazyFrame({"request": requests}).select(
        pl.col("request")
        .pipe(urllib3_requests, session=_HTTPBIN_SESSION)
        .pipe(response_text)
        .str.json_extract(response_dtype)
        .alias("data"),
    )

    ldf2 = pl.LazyFrame(
        {
            "data": [{"args": {"foo": "bar"}, "headers": {"X-Foo": "baz"}}],
        }
    )

    assert_frame_equal(ldf, ldf2)


def test_urllib3_requests_prepare() -> None:
    response_dtype = pl.Struct(
        {
            "args": pl.Struct({"foo": pl.Utf8}),
            "headers": pl.Struct({"X-Foo": pl.Utf8}),
        }
    )

    ldf = pl.LazyFrame({"url": ["https://httpbin.org/get"]}).with_columns(
        pl.col("url")
        .pipe(prepare_request, fields={"foo": "bar"}, headers={"X-Foo": "baz"})
        .pipe(urllib3_requests, session=_HTTPBIN_SESSION)
        .pipe(response_text)
        .str.json_extract(response_dtype)
        .alias("data"),
    )

    ldf2 = pl.LazyFrame(
        {
            "url": ["https://httpbin.org/get"],
            "data": [{"args": {"foo": "bar"}, "headers": {"X-Foo": "baz"}}],
        }
    )

    assert_frame_equal(ldf, ldf2)


def test_urllib3_request_urls_retry_status() -> None:
    session = Session(ok_statuses={200}, retry_statuses={500}, retry_count=10)

    ldf = (
        pl.LazyFrame(
            {
                "url": [
                    "https://httpbin.org/status/200,500",
                    "https://httpbin.org/status/200,500",
                    "https://httpbin.org/status/200,500",
                ]
            }
        )
        .with_columns(
            pl.col("url").pipe(urllib3_request_urls, session=session),
        )
        .select(
            pl.col("url"), pl.col("response").struct.field("status").alias("status")
        )
    )
    ldf2 = pl.LazyFrame(
        {
            "url": [
                "https://httpbin.org/status/200,500",
                "https://httpbin.org/status/200,500",
                "https://httpbin.org/status/200,500",
            ],
            "status": pl.Series([200, 200, 200], dtype=pl.UInt16),
        }
    )
    assert_frame_equal(ldf, ldf2)


def test_urllib3_request_urls_timeout() -> None:
    session = Session(read_timeout=2.0)

    ldf = pl.LazyFrame(
        {
            "url": [
                "https://httpbin.org/delay/1",
                "https://httpbin.org/delay/5",
            ]
        }
    ).with_columns(
        pl.col("url").pipe(urllib3_request_urls, session=session),
    )

    assert ldf.schema == {
        "url": pl.Utf8,
        "response": HTTP_RESPONSE_DTYPE,
    }

    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()
