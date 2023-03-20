# pyright: strict

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.provisional import urls
from hypothesis.strategies import DrawFn, composite
from polars.testing import assert_frame_equal
from polars.testing.parametric import series

from polars_requests import (
    HTTP_REQUEST_DTYPE,
    HTTP_RESPONSE_DTYPE,
    Session,
    prepare_request,
    response_date,
    response_header_value,
    response_ok,
    response_text,
    urllib3_requests,
)

_HTTPBIN_SESSION = Session(connect_timeout=1.0, read_timeout=2.0)


def _st_http_status():
    return st.integers(min_value=100, max_value=599)


def _st_http_header():
    return st.fixed_dictionaries({"name": st.text(), "value": st.text()})


def _st_http_headers():
    return st.lists(_st_http_header())


def _st_http_data():
    return st.binary()


def _st_http_data_utf8():
    return _st_binary_utf8()


@composite
def _st_binary_utf8(draw: DrawFn) -> bytes:
    return draw(st.text()).encode("utf-8")


def _st_http_response_dict(utf8_data: bool = False):
    return st.fixed_dictionaries(
        {
            "status": _st_http_status(),
            "headers": _st_http_headers(),
            "data": _st_http_data_utf8() if utf8_data else _st_http_data(),
        }
    )


def test_urllib3_requests() -> None:
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
            pl.col("url")
            .pipe(prepare_request)
            .pipe(urllib3_requests, session=_HTTPBIN_SESSION, log_group="httpbin"),
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


def test_urllib3_requests_empty() -> None:
    ldf = pl.LazyFrame({"url": []}, schema={"url": pl.Utf8}).with_columns(
        pl.col("url")
        .pipe(prepare_request)
        .pipe(urllib3_requests, session=_HTTPBIN_SESSION, log_group="httpbin"),
    )
    ldf2 = pl.LazyFrame({"url": [], "response": []}).with_columns(
        pl.col("url").cast(pl.Utf8),
        pl.col("response").cast(HTTP_RESPONSE_DTYPE),
    )
    assert_frame_equal(ldf, ldf2)


def test_urllib3_requests_with_defaults() -> None:
    session = Session(fields={"foo": "bar"}, headers={"X-Foo": "baz"})
    response_dtype = pl.Struct(
        {
            "args": pl.Struct({"foo": pl.Utf8}),
            "headers": pl.Struct({"X-Foo": pl.Utf8}),
        }
    )
    ldf = pl.LazyFrame({"url": ["https://httpbin.org/get"]}).with_columns(
        pl.col("url")
        .pipe(prepare_request)
        .pipe(urllib3_requests, session=session, log_group="httpbin")
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
        .pipe(urllib3_requests, session=_HTTPBIN_SESSION, log_group="httpbin")
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


def test_urllib3_requests_prepare_empty_headers() -> None:
    ldf = pl.LazyFrame({"url": ["https://httpbin.org/get"]}).with_columns(
        pl.col("url")
        .pipe(prepare_request)
        .pipe(urllib3_requests, session=_HTTPBIN_SESSION, log_group="httpbin")
        .struct.field("status")
    )
    ldf2 = pl.LazyFrame(
        {
            "url": ["https://httpbin.org/get"],
            "status": pl.Series([200], dtype=pl.UInt16),
        }
    )
    assert_frame_equal(ldf, ldf2)


@pytest.mark.xfail
def test_urllib3_requests_prepare_empty_df() -> None:
    ldf = pl.LazyFrame({"url": pl.Series([], dtype=pl.Utf8)}).with_columns(
        pl.col("url")
        .pipe(prepare_request, fields={"foo": "bar"}, headers={"X-Foo": "baz"})
        .pipe(urllib3_requests, session=_HTTPBIN_SESSION, log_group="httpbin")
        .struct.field("status")
    )
    ldf2 = pl.LazyFrame(
        {
            "url": pl.Series([], dtype=pl.Utf8),
            "status": pl.Series([], dtype=pl.UInt16),
        }
    )
    assert_frame_equal(ldf, ldf2)


@pytest.mark.xfail
def test_urllib3_requests_prepare_empty_df_and_headers() -> None:
    ldf = pl.LazyFrame({"url": pl.Series([], dtype=pl.Utf8)}).with_columns(
        pl.col("url")
        .pipe(prepare_request)
        .pipe(urllib3_requests, session=_HTTPBIN_SESSION, log_group="httpbin")
        .struct.field("status")
    )
    ldf2 = pl.LazyFrame(
        {
            "url": pl.Series([], dtype=pl.Utf8),
            "status": pl.Series([], dtype=pl.UInt16),
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
        .pipe(urllib3_requests, session=_HTTPBIN_SESSION, log_group="httpbin")
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


def test_urllib3_requests_retry_status() -> None:
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
            pl.col("url")
            .pipe(prepare_request)
            .pipe(urllib3_requests, session=session, log_group="httpbin"),
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


def test_urllib3_requests_timeout() -> None:
    session = Session(read_timeout=2.0)

    ldf = pl.LazyFrame(
        {
            "url": [
                "https://httpbin.org/delay/1",
                "https://httpbin.org/delay/5",
            ]
        }
    ).with_columns(
        pl.col("url")
        .pipe(prepare_request)
        .pipe(urllib3_requests, session=session, log_group="httpbin"),
    )

    assert ldf.schema == {
        "url": pl.Utf8,
        "response": HTTP_RESPONSE_DTYPE,
    }

    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()


@given(
    url=urls(),
    fields=st.dictionaries(st.text(), st.text()),
    headers=st.dictionaries(st.text(), st.text()),
)
def test_prepare_request(
    url: str,
    fields: dict[str, pl.Expr | str],
    headers: dict[str, pl.Expr | str],
) -> None:
    expr = prepare_request(url, fields=fields, headers=headers)
    df = pl.select(expr)
    assert df.schema == {"request": HTTP_REQUEST_DTYPE}


@given(
    responses=series(
        dtype=HTTP_RESPONSE_DTYPE,
        strategy=_st_http_response_dict(),
    )
)
def test_response_ok(responses: pl.Series) -> None:
    df = pl.DataFrame({"response": responses}).select(
        pl.col("response").pipe(response_ok)
    )
    assert df.schema == {"ok": pl.Boolean}
    assert len(df) == len(responses)


@given(
    responses=series(
        dtype=HTTP_RESPONSE_DTYPE,
        strategy=_st_http_response_dict(),
    )
)
def test_response_header_value(responses: pl.Series) -> None:
    df = pl.DataFrame({"response": responses}).select(
        pl.col("response").pipe(response_header_value, name="Date")
    )
    assert df.schema == {"Date": pl.Utf8}
    assert len(df) == len(responses)


@given(
    responses=series(
        dtype=HTTP_RESPONSE_DTYPE,
        strategy=_st_http_response_dict(utf8_data=True),
    )
)
def test_response_text(responses: pl.Series) -> None:
    df = pl.DataFrame({"response": responses}).select(
        pl.col("response").pipe(response_text)
    )
    assert df.schema == {"response_text": pl.Utf8}
    assert len(df) == len(responses)
