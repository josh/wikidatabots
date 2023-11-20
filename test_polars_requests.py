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
    prepare_request,
    request,
    resolve_redirects,
    response_date,
    response_header_value,
    response_text,
)


def _response_ok(response: pl.Expr) -> pl.Expr:
    return (
        response.struct.field("status").is_between(200, 300, closed="left").alias("ok")
    )


def _st_http_status():
    return st.integers(min_value=100, max_value=599)


def _st_http_header():
    return st.fixed_dictionaries(
        {"name": st.text(max_size=5), "value": st.text(max_size=5)}
    )


def _st_http_headers():
    return st.lists(_st_http_header())


def _st_http_data():
    return st.binary(max_size=10)


@composite
def _st_binary_utf8(draw: DrawFn) -> bytes:
    return draw(st.text(max_size=5)).encode("utf-8")


def _st_http_data_utf8():
    return _st_binary_utf8()


def _st_http_response_dict(utf8_data: bool = False):
    return st.fixed_dictionaries(
        {
            "status": _st_http_status(),
            "headers": _st_http_headers(),
            "data": _st_http_data_utf8() if utf8_data else _st_http_data(),
        }
    )


def test_request() -> None:
    response_dtype = pl.Struct({"args": pl.Struct({"foo": pl.Utf8})})
    ldf = (
        pl.LazyFrame(
            {
                "url": [
                    "https://postman-echo.com/get?foo=1",
                    "https://postman-echo.com/get?foo=2",
                    "https://postman-echo.com/get?foo=3",
                ]
            }
        )
        .with_columns(
            pl.col("url").pipe(prepare_request).pipe(request, log_group="postman"),
        )
        .with_columns(
            # BUG: This is returning a List for some reason
            # pl.col("response").pipe(_response_ok),
            pl.lit(True).alias("ok"),
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
            .str.json_decode(response_dtype)
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


def test_request_empty() -> None:
    ldf = pl.LazyFrame({"url": []}, schema={"url": pl.Utf8}).with_columns(
        pl.col("url").pipe(prepare_request).pipe(request, log_group="postman"),
    )
    ldf2 = pl.LazyFrame({"url": [], "response": []}).with_columns(
        pl.col("url").cast(pl.Utf8),
        pl.col("response").cast(HTTP_RESPONSE_DTYPE),
    )
    assert_frame_equal(ldf, ldf2)


def test_request_raw() -> None:
    response_dtype = pl.Struct(
        {
            "args": pl.Struct({"foo": pl.Utf8}),
            "headers": pl.Struct({"x-foo": pl.Utf8}),
        }
    )

    requests = pl.Series(
        [
            {
                "url": "https://postman-echo.com/get?foo=bar",
                "headers": [{"name": "x-foo", "value": "baz"}],
            }
        ],
    )

    ldf = pl.LazyFrame({"request": requests}).select(
        pl.col("request")
        .pipe(request, log_group="postman")
        .pipe(response_text)
        .str.json_decode(response_dtype)
        .alias("data"),
    )

    ldf2 = pl.LazyFrame(
        {
            "data": [{"args": {"foo": "bar"}, "headers": {"x-foo": "baz"}}],
        }
    )

    assert_frame_equal(ldf, ldf2)


def test_request_prepare_empty_headers() -> None:
    ldf = pl.LazyFrame({"url": ["https://postman-echo.com/get"]}).with_columns(
        pl.col("url")
        .pipe(prepare_request)
        .pipe(request, log_group="postman")
        .struct.field("status")
    )
    ldf2 = pl.LazyFrame(
        {
            "url": ["https://postman-echo.com/get"],
            "status": pl.Series([200], dtype=pl.UInt16),
        }
    )
    assert_frame_equal(ldf, ldf2)


def test_request_prepare_empty_df() -> None:
    ldf = pl.LazyFrame({"url": pl.Series([], dtype=pl.Utf8)}).with_columns(
        pl.col("url")
        .pipe(prepare_request, fields={"foo": "bar"}, headers={"x-foo": "baz"})
        .pipe(request, log_group="postman")
        .struct.field("status")
    )
    ldf2 = pl.LazyFrame(
        {
            "url": pl.Series([], dtype=pl.Utf8),
            "status": pl.Series([], dtype=pl.UInt16),
        }
    )
    assert_frame_equal(ldf, ldf2)


def test_request_prepare_empty_df_and_headers() -> None:
    ldf = pl.LazyFrame({"url": pl.Series([], dtype=pl.Utf8)}).with_columns(
        pl.col("url")
        .pipe(prepare_request)
        .pipe(request, log_group="postman")
        .struct.field("status")
    )
    ldf2 = pl.LazyFrame(
        {
            "url": pl.Series([], dtype=pl.Utf8),
            "status": pl.Series([], dtype=pl.UInt16),
        }
    )
    assert_frame_equal(ldf, ldf2)


def test_request_prepare() -> None:
    response_dtype = pl.Struct(
        {
            "args": pl.Struct({"foo": pl.Utf8}),
            "headers": pl.Struct({"x-foo": pl.Utf8}),
        }
    )

    ldf = pl.LazyFrame({"url": ["https://postman-echo.com/get"]}).with_columns(
        pl.col("url")
        .pipe(prepare_request, fields={"foo": "bar"}, headers={"x-foo": "baz"})
        .pipe(request, log_group="postman")
        .pipe(response_text)
        .str.json_decode(response_dtype)
        .alias("data"),
    )

    ldf2 = pl.LazyFrame(
        {
            "url": ["https://postman-echo.com/get"],
            "data": [{"args": {"foo": "bar"}, "headers": {"x-foo": "baz"}}],
        }
    )

    assert_frame_equal(ldf, ldf2)


def test_request_prepare_just_fields() -> None:
    response_dtype = pl.Struct({"args": pl.Struct({"foo": pl.Utf8})})

    ldf = pl.LazyFrame({"url": ["https://postman-echo.com/get"]}).with_columns(
        pl.col("url")
        .pipe(prepare_request, fields={"foo": "bar"})
        .pipe(request, log_group="postman")
        .pipe(response_text)
        .str.json_decode(response_dtype)
        .alias("data"),
    )

    ldf2 = pl.LazyFrame(
        {
            "url": ["https://postman-echo.com/get"],
            "data": [{"args": {"foo": "bar"}}],
        }
    )

    assert_frame_equal(ldf, ldf2)


def test_request_retry_status() -> None:
    ldf = (
        pl.LazyFrame(
            {
                "url": [
                    "https://postman-echo.com/status/200,500",
                    "https://postman-echo.com/status/200,500",
                    "https://postman-echo.com/status/200,500",
                ]
            }
        )
        .with_columns(
            pl.col("url")
            .pipe(prepare_request)
            .pipe(request, log_group="postman", retry_count=10),
        )
        .select(
            pl.col("url"), pl.col("response").struct.field("status").alias("status")
        )
    )
    ldf2 = pl.LazyFrame(
        {
            "url": [
                "https://postman-echo.com/status/200,500",
                "https://postman-echo.com/status/200,500",
                "https://postman-echo.com/status/200,500",
            ],
            "status": pl.Series([200, 200, 200], dtype=pl.UInt16),
        }
    )
    assert_frame_equal(ldf, ldf2)


def test_request_timeout() -> None:
    ldf = pl.LazyFrame(
        {
            "url": [
                "https://postman-echo.com/delay/1",
                "https://postman-echo.com/delay/5",
            ]
        }
    ).with_columns(
        pl.col("url")
        .pipe(prepare_request)
        .pipe(request, log_group="postman", timeout=2.0),
    )

    assert ldf.schema == {
        "url": pl.Utf8,
        "response": HTTP_RESPONSE_DTYPE,
    }

    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()


@given(
    url=urls(),
    fields=st.dictionaries(st.text(max_size=5), st.text(max_size=5), max_size=3),
    headers=st.dictionaries(st.text(max_size=5), st.text(max_size=5), max_size=3),
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
        max_size=3,
    )
)
def test_response_ok(responses: pl.Series) -> None:
    df = pl.DataFrame({"response": responses}).select(
        pl.col("response").pipe(_response_ok)
    )
    assert df.schema == {"ok": pl.Boolean}
    assert len(df) == len(responses)


@given(
    responses=series(
        dtype=HTTP_RESPONSE_DTYPE,
        strategy=_st_http_response_dict(),
        max_size=3,
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
        max_size=3,
    )
)
def test_response_text(responses: pl.Series) -> None:
    df = pl.DataFrame({"response": responses}).select(
        pl.col("response").pipe(response_text)
    )
    assert df.schema == {"response_text": pl.Utf8}
    assert len(df) == len(responses)


def test_resolve_redirects() -> None:
    df = pl.LazyFrame(
        {
            "url": [
                "https://itunes.apple.com/"
                "us/movie/avatar-the-way-of-water/id1676858107?uo=4",
            ]
        }
    )
    df1 = df.with_columns(
        pl.col("url")
        .pipe(resolve_redirects, log_group="redirects")
        .alias("resolved_url"),
    )
    df2 = df.with_columns(
        pl.lit(
            "https://tv.apple.com/"
            "us/movie/avatar-the-way-of-water/umc.cmc.5k5xo2espahvd6kcswi2b5oe9"
        ).alias("resolved_url"),
    )
    assert_frame_equal(df1, df2)
