# pyright: strict

import polars as pl

from polars_requests import (
    Session,
    request_url_expr,
    request_url_expr_text,
    request_url_ldf,
    response_expr_text,
)

_HTTPBIN_SESSION = Session(host="httpbin.org", connect_timeout=1.0, read_timeout=2.0)


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
        pl.col("url")
        .pipe(request_url_expr_text, session=_HTTPBIN_SESSION)
        .alias("response_text"),
    )
    assert df.shape == (3, 2)
    assert df.schema == {"url": pl.Utf8, "response_text": pl.Utf8}


def test_request_url_ldf() -> None:
    ldf = request_url_ldf("https://httpbin.org/get?foo=1", session=_HTTPBIN_SESSION)
    assert ldf.schema == {"response": pl.Object}
    assert len(ldf.collect()) == 1


def test_response_expr_text() -> None:
    df = (
        pl.DataFrame({"url": ["https://httpbin.org/get"]})
        .with_columns(
            pl.col("url")
            .pipe(request_url_expr, session=_HTTPBIN_SESSION)
            .alias("response"),
        )
        .with_columns(
            pl.col("response").pipe(response_expr_text).alias("response_text"),
        )
    )
    assert df.shape == (1, 3)
    assert df.schema == {
        "url": pl.Utf8,
        "response": pl.Object,
        "response_text": pl.Utf8,
    }
