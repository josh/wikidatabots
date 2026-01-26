from collections.abc import Callable
from typing import TypeVar

import polars as pl
from hypothesis import given
from polars.testing import assert_frame_equal
from polars.testing.parametric import series

from polars_utils import (
    apply_with_tqdm,
    now,
    sample,
)


def setup_module() -> None:
    pl.enable_string_cache()


def teardown_module() -> None:
    pl.disable_string_cache()


T = TypeVar("T")


def assert_called_once() -> Callable[[T], T]:
    calls: int = 1

    def mock(value: T) -> T:
        nonlocal calls
        calls -= 1
        assert calls >= 0, "mock called too many times"
        return value

    return mock


def test_now() -> None:
    df = pl.LazyFrame({"a": [1, 2, 3]}).with_columns(
        now().alias("timestamp"),
    )
    assert df.collect_schema() == pl.Schema(
        {
            "a": pl.Int64(),
            "timestamp": pl.Datetime(time_unit="ms"),
        }
    )
    df.collect()


def test_sample() -> None:
    df = pl.LazyFrame({"a": [1, 2, 3, 4, 5]})
    assert len(df.pipe(sample, n=3).collect()) == 3


def test_apply_with_tqdm() -> None:
    df1 = pl.LazyFrame({"s": [1, 2, 3]})
    df2 = pl.LazyFrame({"s": [2, 3, 4]})
    df3 = df1.select(
        apply_with_tqdm(
            pl.col("s"),
            lambda x: x + 1,
            return_dtype=pl.Int64,
            log_group="test",
        )
    )
    assert df3.collect_schema() == pl.Schema({"s": pl.Int64})
    assert_frame_equal(df2, df3)


@given(s=series(dtype=pl.Int64))
def test_apply_with_tqdm_properties(s: pl.Series) -> None:
    def fn(a: int) -> int:
        return max(min(a, 0), 100)

    df = pl.DataFrame({"a": s}).select(
        pl.col("a").pipe(apply_with_tqdm, fn, return_dtype=pl.Int64, log_group="test")
    )
    assert df.collect_schema() == pl.Schema({"a": pl.Int64})
    assert len(df) == len(s)
