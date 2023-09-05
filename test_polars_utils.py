# pyright: strict

from math import ceil
from typing import Callable, Iterator, TypeVar

import polars as pl
import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from polars.testing import assert_frame_equal, assert_series_equal
from polars.testing.parametric import column, dataframes, series

from polars_utils import (
    align_to_index,
    apply_with_tqdm,
    compute_stats,
    csv_extract,
    expr_indicies_sorted,
    frame_diff,
    groups_of,
    map_streaming,
    merge_with_indicator,
    now,
    position_weights,
    pyformat,
    sample,
    update_or_append,
    weighted_random,
    weighted_sample,
    xml_extract,
)


def setup_module() -> None:
    pl.enable_string_cache(True)


def teardown_module() -> None:
    pl.enable_string_cache(False)


T = TypeVar("T")


def assert_called_once() -> Callable[[T], T]:
    calls: int = 1

    def mock(value: T) -> T:
        nonlocal calls
        calls -= 1
        assert calls >= 0, "mock called too many times"
        return value

    return mock


def test_pyformat() -> None:
    df = pl.DataFrame({"a": ["a", "b", "c"], "b": [1, 2, 3]})

    df1 = df.select([pyformat("foo_{}_bar_{}", pl.col("a"), "b").alias("fmt")])
    df2 = pl.DataFrame({"fmt": ["foo_a_bar_1", "foo_b_bar_2", "foo_c_bar_3"]})
    assert_frame_equal(df1, df2)

    df1 = df.select([pyformat("foo_{0}_bar_{1}", pl.col("a"), "b").alias("fmt")])
    df2 = pl.DataFrame({"fmt": ["foo_a_bar_1", "foo_b_bar_2", "foo_c_bar_3"]})
    assert_frame_equal(df1, df2)

    df1 = df.select([pyformat("foo_{a}_bar_{b}", a=pl.col("a"), b="b").alias("fmt")])
    df2 = pl.DataFrame({"fmt": ["foo_a_bar_1", "foo_b_bar_2", "foo_c_bar_3"]})
    assert_frame_equal(df1, df2)

    df1 = df.select([pyformat("foo_{}_bar_{b}", pl.col("a"), b="b").alias("fmt")])
    df2 = pl.DataFrame({"fmt": ["foo_a_bar_1", "foo_b_bar_2", "foo_c_bar_3"]})
    assert_frame_equal(df1, df2)

    df = pl.DataFrame({"a": ["a", None, "c"], "b": [1, 2, None]})
    df1 = df.select([pyformat("foo_{}_bar_{}", pl.col("a"), "b").alias("fmt")])
    df2 = pl.DataFrame({"fmt": ["foo_a_bar_1", None, None]})
    assert_frame_equal(df1, df2)


def test_merge_with_indicator() -> None:
    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 2, 3]}).map_batches(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [2, 3, 4], "b": [3, 3, 4]}).map_batches(
        assert_called_once()
    )

    df3 = merge_with_indicator(df1, df2, on="a")
    df4 = pl.LazyFrame(
        {
            "a": [2, 3, 4, 1],
            "b": [2, 3, None, 1],
            "b_right": [3, 3, 4, None],
            "_merge": ["both", "both", "right_only", "left_only"],
        },
        schema={
            "a": pl.Int64,
            "b": pl.Int64,
            "b_right": pl.Int64,
            "_merge": pl.Categorical,
        },
    )
    assert_frame_equal(df3, df4)


def test_now() -> None:
    df = pl.LazyFrame({"a": [1, 2, 3]}).with_columns(
        now().alias("timestamp"),
    )
    assert df.schema == {"a": pl.Int64, "timestamp": pl.Datetime}
    df.collect()


@given(df=dataframes(lazy=True, max_cols=5, min_size=3, max_size=20))
@settings(max_examples=5)
def test_sample(df: pl.LazyFrame) -> None:
    assert len(df.pipe(sample, n=3).collect()) == 3


def test_position_weights() -> None:
    def _eval_weights(size: int) -> pl.Series:
        return (
            pl.Series(range(size), dtype=pl.UInt32)
            .to_frame()
            .select(position_weights().alias("weights"))
            .to_series()
        )

    assert_series_equal(_eval_weights(0), pl.Series("weights", [], dtype=pl.Float64))
    assert_series_equal(_eval_weights(1), pl.Series("weights", [1.0], dtype=pl.Float64))
    assert_series_equal(
        _eval_weights(2), pl.Series("weights", [2 / 3, 1 / 3], dtype=pl.Float64)
    )
    assert_series_equal(
        _eval_weights(3), pl.Series("weights", [1 / 2, 1 / 3, 1 / 6], dtype=pl.Float64)
    )
    assert_series_equal(
        _eval_weights(4),
        pl.Series("weights", [2 / 5, 3 / 10, 1 / 5, 1 / 10], dtype=pl.Float64),
    )
    assert_series_equal(
        _eval_weights(5),
        pl.Series("weights", [1 / 3, 4 / 15, 1 / 5, 2 / 15, 1 / 15], dtype=pl.Float64),
    )


def test_weighted_random() -> None:
    df = (
        pl.DataFrame({"a": [1, 2, 3]})
        .with_columns(
            position_weights().pipe(weighted_random).alias("sort_arg"),
        )
        .sort("sort_arg")
    )
    assert df.schema == {"a": pl.Int64, "sort_arg": pl.UInt32}
    assert len(df) == 3


def test_weighted_sample() -> None:
    df = pl.LazyFrame({"a": [1, 2, 3]}).pipe(weighted_sample, n=2).collect()
    assert len(df) == 2


def test_csv_extract() -> None:
    dtype = pl.List(pl.Struct({"a": pl.UInt8}))
    df1 = (
        pl.LazyFrame({"text": ["a\n1\n2\n3\n", "a\n4\n5\n6\n", "a\n7\n8\n9\n"]})
        .select(
            pl.col("text").cast(pl.Binary).pipe(csv_extract, dtype=dtype).alias("data"),
        )
        .explode("data")
        .select(pl.col("data").struct.field("a"))
    )
    df2 = pl.LazyFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9]}, schema={"a": pl.UInt8})
    assert_frame_equal(df1, df2)


XML_EXAMPLE = """
<data>
    <country name="Liechtenstein">
        <rank>1</rank>
        <year>2008</year>
        <gdppc>141100</gdppc>
        <neighbor name="Austria" direction="E"/>
        <neighbor name="Switzerland" direction="W"/>
    </country>
    <country name="Singapore">
        <rank>4</rank>
        <year>2011</year>
        <gdppc>59900</gdppc>
        <neighbor name="Malaysia" direction="N"/>
    </country>
    <country name="Panama">
        <rank>68</rank>
        <year>2011</year>
        <gdppc>13600</gdppc>
        <neighbor name="Costa Rica" direction="W"/>
        <neighbor name="Colombia" direction="E"/>
    </country>
</data>
"""


def test_xml_extract() -> None:
    dtype = pl.List(pl.Struct({"name": pl.Utf8, "year": pl.Int64}))

    df = (
        pl.DataFrame({"xml": [XML_EXAMPLE]})
        .select(
            pl.col("xml").pipe(xml_extract, dtype).alias("country"),
        )
        .explode("country")
        .unnest("country")
    )

    df2 = pl.DataFrame(
        {"name": ["Liechtenstein", "Singapore", "Panama"], "year": [2008, 2011, 2011]}
    )

    assert_frame_equal(df, df2)


def test_align_to_index():
    df1 = pl.LazyFrame([], schema={"id": pl.Int64})
    assert_frame_equal(align_to_index(df1, name="id"), df1)

    df1 = pl.LazyFrame(
        {
            "id": pl.Series([1, 2, 5], dtype=pl.Int8),
            "value": [1, 2, 5],
        }
    ).map_batches(assert_called_once())
    df2 = pl.LazyFrame(
        {
            "id": pl.Series([0, 1, 2, 3, 4, 5], dtype=pl.Int8),
            "value": [None, 1, 2, None, None, 5],
        }
    ).map_batches(assert_called_once())
    assert_frame_equal(align_to_index(df1, name="id"), df2)

    df1 = pl.LazyFrame(
        {
            "id": pl.Series([255], dtype=pl.UInt8),
            "value": [42],
        }
    ).map_batches(assert_called_once())
    df2 = align_to_index(df1, name="id").collect()
    assert df2.schema == {"id": pl.UInt8, "value": pl.Int64}
    assert df2.height == 256

    df = pl.LazyFrame(
        {
            "id": [-1, 2, 5],
            "value": [-1, 2, 5],
        }
    )
    with pytest.raises(pl.ComputeError):  # type: ignore
        align_to_index(df, name="id").collect()

    df = pl.LazyFrame(
        {
            "id": ["a", "b", "c"],
            "value": [1, 2, 5],
        }
    )
    with pytest.raises(pl.ComputeError):  # type: ignore
        align_to_index(df, name="id").collect()


@given(
    df=dataframes(
        cols=[
            column("a", dtype=pl.UInt8, unique=True),
            column("b", dtype=pl.UInt16, unique=True),
            column("c", dtype=pl.Boolean),
        ],
        lazy=True,
    )
)
def test_align_to_index_properties(df: pl.LazyFrame):
    df2 = align_to_index(df, name="a").collect()

    df2 = align_to_index(df, name="b").collect()
    assert df2.height >= df.collect().height


def test_align_to_index_evaluates_df_once():
    ldf1 = pl.LazyFrame(
        {
            "id": pl.Series([1, 2, 5], dtype=pl.Int8),
            "value": [1, 2, 5],
        }
    ).map_batches(assert_called_once())
    align_to_index(ldf1, name="id").collect()

    ldf2 = pl.LazyFrame(
        {
            "id": pl.Series([1, 2, 5], dtype=pl.Int8),
            "value": [1, 2, 5],
        }
    ).select(
        [
            pl.col("id").map_batches(assert_called_once(), return_dtype=pl.Int8),
            pl.col("value").map_batches(assert_called_once(), return_dtype=pl.Int64),
        ]
    )
    align_to_index(ldf2, name="id").collect()


def test_update_or_append() -> None:
    df1 = pl.LazyFrame({"a": []}).map_batches(assert_called_once())
    df2 = pl.LazyFrame({"a": []}).map_batches(assert_called_once())
    df3 = pl.LazyFrame({"a": []})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1]}).map_batches(assert_called_once())
    df2 = pl.LazyFrame({"a": [1]}).map_batches(assert_called_once())
    df3 = pl.LazyFrame({"a": [1]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1]}).map_batches(assert_called_once())
    df2 = pl.LazyFrame({"a": [2]}).map_batches(assert_called_once())
    df3 = pl.LazyFrame({"a": [1, 2]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1], "b": [True]}).map_batches(assert_called_once())
    df2 = pl.LazyFrame({"a": [2]}).map_batches(assert_called_once())
    df3 = pl.LazyFrame({"a": [1, 2], "b": [True, None]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1], "b": [True]}).map_batches(assert_called_once())
    df2 = pl.LazyFrame({"a": [2], "b": [False]}).map_batches(assert_called_once())
    df3 = pl.LazyFrame({"a": [1, 2], "b": [True, False]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1, 2], "b": [True, True]}).map_batches(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [3], "b": [False]}).map_batches(assert_called_once())
    df3 = pl.LazyFrame({"a": [1, 2, 3], "b": [True, True, False]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1], "b": [1], "c": [True]}).map_batches(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [1], "b": [2]}).map_batches(assert_called_once())
    df3 = pl.LazyFrame({"a": [1], "b": [2], "c": [True]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1], "b": [1], "c": [True]}).map_batches(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [1], "b": [2], "c": [False]}).map_batches(
        assert_called_once()
    )
    df3 = pl.LazyFrame({"a": [1], "b": [2], "c": [False]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)


@given(
    df1=dataframes(
        cols=[
            column("a", dtype=pl.UInt8, null_probability=0.0, unique=True),
            column("b", dtype=pl.UInt8),
            column("c", dtype=pl.Boolean),
        ],
    ),
    df2=dataframes(
        cols=[
            column("a", dtype=pl.UInt8, null_probability=0.0, unique=True),
            column("b", dtype=pl.UInt8),
            column("c", dtype=pl.Boolean),
        ],
    ),
)
def test_update_or_append_properties(df1: pl.DataFrame, df2: pl.DataFrame) -> None:
    assume(df1.height == 0 or df1["a"].is_not_null().all())
    assume(df2.height == 0 or df2["a"].is_not_null().all())
    assume(df1.height == 0 or df1["a"].is_unique().all())
    assume(df2.height == 0 or df2["a"].is_unique().all())

    df3 = update_or_append(df1.lazy(), df2.lazy(), on="a").collect()
    assert df3.schema == df1.schema
    assert df3.columns == df1.columns
    assert df3.height >= df1.height
    assert df3.height >= df2.height
    assert df3.height == 0 or df3["a"].is_not_null().all()
    assert df3.height == 0 or df3["a"].is_unique().all()


def test_frame_diff() -> None:
    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": [False, False, False]}).map_batches(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [2, 3, 4], "b": [True, False, False]}).map_batches(
        assert_called_once()
    )
    added, removed, updated, b_updated = frame_diff(df1, df2, on="a").collect().row(0)
    assert added == 1
    assert removed == 1
    assert updated == 1
    assert b_updated == 1

    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": [False, False, False]}).map_batches(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [1, 2, 3], "b": [True, True, False]}).map_batches(
        assert_called_once()
    )
    added, removed, updated, b_updated = frame_diff(df1, df2, on="a").collect().row(0)
    assert added == 0
    assert removed == 0
    assert updated == 2
    assert b_updated == 2


df_st = dataframes(
    cols=[
        column("a", dtype=pl.Int64, unique=True),
        column("b", dtype=pl.Boolean),
        column("c", dtype=pl.Boolean),
    ],
    max_size=25,
)


@given(df1=df_st, df2=df_st)
def test_frame_diff_properties(df1: pl.DataFrame, df2: pl.DataFrame) -> None:
    ldf = frame_diff(
        df1.lazy().map_batches(assert_called_once()),
        df2.lazy().map_batches(assert_called_once()),
        on="a",
    )
    assert ldf.columns[0:3] == ["added", "removed", "updated"]
    assert ldf.schema == {
        "added": pl.UInt32,
        "removed": pl.UInt32,
        "updated": pl.UInt32,
        "b_updated": pl.UInt32,
        "c_updated": pl.UInt32,
    }
    df = ldf.collect()
    assert len(df) == 1
    row = df.row(0)
    assert len(row) == 5
    added, removed, updated, b_updated, c_updated = row
    assert added >= 0, "added should be >= 0"
    assert added <= len(df2), "added should be <= len(df2)"
    assert removed >= 0, "removed should be >= 0"
    assert removed <= len(df1), "removed should be <= len(df1)"
    assert updated >= 0, "updated should be >= 0"
    assert updated <= len(df1), "updated should be <= len(df1)"
    assert b_updated >= 0, "b_updated should be >= 0"
    assert b_updated <= updated, "b_updated should be <= updated"
    assert c_updated >= 0, "b_updated should be >= 0"
    assert c_updated <= updated, "b_updated should be <= updated"
    assert df1.height - removed + added == df2.height, "df1 - removed + added == df2"
    assert df2.height - added + removed == df1.height, "df2 - added + removed == df1"


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
    assert df3.schema == {"s": pl.Int64}
    assert_frame_equal(df2, df3)


@given(s=series(dtype=pl.Int64))
def test_apply_with_tqdm_properties(s: pl.Series) -> None:
    def fn(a: int) -> int:
        return max(min(a, 0), 100)

    df = pl.DataFrame({"a": s}).select(
        pl.col("a").pipe(apply_with_tqdm, fn, return_dtype=pl.Int64, log_group="test")
    )
    assert df.schema == {"a": pl.Int64}
    assert len(df) == len(s)


@given(
    df=dataframes(cols=[column("a", dtype=pl.Int64)], max_size=20, lazy=True),
    chunk_size=st.integers(min_value=1, max_value=25),
)
def test_map_streaming(df: pl.LazyFrame, chunk_size: int) -> None:
    expected_df = df.select(pl.col("a") + 2)
    actual_df = df.pipe(
        map_streaming,
        pl.col("a") + 2,
        return_schema={"a": pl.Int64},
        chunk_size=chunk_size,
    )
    assert_frame_equal(expected_df, actual_df)


@given(
    df=dataframes(cols=[column("a", dtype=pl.Int64)], max_size=20, lazy=True),
    chunk_size=st.integers(min_value=1, max_value=25),
)
def test_map_streaming_parallel(df: pl.LazyFrame, chunk_size: int) -> None:
    expected_df = df.select(pl.col("a") + 3)
    actual_df = df.pipe(
        map_streaming,
        pl.col("a") + 3,
        return_schema={"a": pl.Int64},
        chunk_size=chunk_size,
        parallel=True,
    )
    assert_frame_equal(expected_df, actual_df)


def test_groups_of() -> None:
    df1 = pl.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    df2 = df1.select(pl.col("a").pipe(groups_of, n=2))
    df3 = pl.DataFrame({"a": [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]})
    assert_frame_equal(df2, df3)

    df1 = pl.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    df2 = df1.select(pl.col("a").pipe(groups_of, n=3))
    df3 = pl.DataFrame({"a": [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]})
    assert_frame_equal(df2, df3)

    df1 = pl.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    df2 = df1.select(pl.col("a").pipe(groups_of, n=100))
    df3 = pl.DataFrame({"a": [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]})
    assert_frame_equal(df2, df3)


@given(
    df=dataframes(cols=[column("a", dtype=pl.Int64)], max_size=10),
    n=st.integers(min_value=1, max_value=15),
)
def test_groups_of_properties(df: pl.DataFrame, n: int) -> None:
    df2 = df.select(pl.col("a").pipe(groups_of, n=n))

    assert len(df2) == ceil(len(df) / n)

    for row in df2.to_dicts():
        assert row["a"]
        assert len(row["a"]) > 0
        assert len(row["a"]) <= n


def _lst_indicies(a: list[int], b: list[int]) -> Iterator[int | None]:
    for e in a:
        try:
            yield b.index(e)
        except ValueError:
            yield None


_1_TO_10 = st.integers(min_value=0, max_value=10)
_1_TO_10_OR_NONE = st.one_of(st.none(), _1_TO_10)


@given(
    a=st.lists(_1_TO_10_OR_NONE, min_size=0, max_size=10),
    b=st.lists(_1_TO_10, min_size=1, max_size=10),
)
def test_indices_sorted(a: list[int], b: list[int]) -> None:
    b = sorted(b)
    c = list(_lst_indicies(a, b))

    a_s = pl.Series(a, dtype=pl.UInt32)
    b_s = pl.Series(b, dtype=pl.UInt32)

    s = pl.select(expr_indicies_sorted(pl.lit(a_s), pl.lit(b_s))).to_series()
    assert s.dtype == pl.UInt32
    assert len(s) == len(a)
    assert c == s.to_list()


@given(
    df=dataframes(max_cols=20, max_size=10, null_probability=0.1),
)
@settings(max_examples=10)
def test_compute_stats(df: pl.DataFrame) -> None:
    stats_df = compute_stats(df)
    assert len(stats_df) == len(df.columns)


def test_read_parquet_s3():
    pl.read_parquet(
        "s3://wikidatabots/plex.parquet",
        columns=["key", "retrieved_at"],
        storage_options={"anon": True},
    ).lazy().collect()


def test_scan_parquet_s3():
    pl.scan_parquet(
        "s3://wikidatabots/plex.parquet",
        storage_options={"anon": True},
    ).select(["key", "retrieved_at"]).collect()
