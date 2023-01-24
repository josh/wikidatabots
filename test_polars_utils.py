# pyright: strict

import polars as pl
import pytest
from hypothesis import given
from polars.testing import assert_frame_equal, assert_series_equal
from polars.testing.parametric import column, dataframes

from polars_utils import (
    align_to_index,
    parse_json,
    request_text,
    row_differences,
    unique_row_differences,
)


def test_align_to_index():
    df1 = pl.DataFrame([], columns={"id": pl.Int64}).lazy()
    assert_frame_equal(align_to_index(df1, name="id"), df1)

    df1 = pl.DataFrame(
        {
            "id": pl.Series([1, 2, 5], dtype=pl.Int8),
            "value": [1, 2, 5],
        }
    ).lazy()
    df2 = pl.DataFrame(
        {
            "id": pl.Series([0, 1, 2, 3, 4, 5], dtype=pl.Int8),
            "value": [None, 1, 2, None, None, 5],
        }
    ).lazy()
    assert_frame_equal(align_to_index(df1, name="id"), df2)

    df1 = pl.DataFrame(
        {
            "id": pl.Series([255], dtype=pl.UInt8),
            "value": [42],
        }
    ).lazy()
    df2 = align_to_index(df1, name="id").collect()
    assert df2.schema == {"id": pl.UInt8, "value": pl.Int64}
    assert df2.height == 256

    # df = pl.DataFrame(
    #     {
    #         "id": [-1, 2, 5],
    #         "value": [-1, 2, 5],
    #     }
    # ).lazy()
    # align_to_index(df, name="id").collect()

    df = pl.DataFrame(
        {
            "id": ["a", "b", "c"],
            "value": [1, 2, 5],
        }
    ).lazy()
    with pytest.raises(AssertionError):
        align_to_index(df, name="id").collect()


@given(
    df=dataframes(
        cols=[
            column("a", dtype=pl.UInt8, unique=True),
            column("b", dtype=pl.UInt16, unique=True),
            column("c", dtype=pl.Boolean),
        ]
    )
)
def test_align_to_index_properties(df: pl.DataFrame):
    df2 = align_to_index(df.lazy(), name="a").collect()
    assert df2.height >= df.height

    df2 = align_to_index(df.lazy(), name="b").collect()
    assert df2.height >= df.height


def test_row_differences():
    df1 = pl.DataFrame({"a": [1, 2, 3]}).lazy()
    df2 = pl.DataFrame({"a": [2, 3, 4]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 1
    assert removed == 1

    df1 = pl.DataFrame({"a": [1]}).lazy()
    df2 = pl.DataFrame({"a": [1, 2, 3, 4]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 3
    assert removed == 0

    df1 = pl.DataFrame({"a": [1, 2, 3, 4]}).lazy()
    df2 = pl.DataFrame({"a": [1]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 0
    assert removed == 3

    df1 = pl.DataFrame({"a": [1]}).lazy()
    df2 = pl.DataFrame({"a": [1, 1]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 1
    assert removed == 0

    df1 = pl.DataFrame({"a": [1, 1]}).lazy()
    df2 = pl.DataFrame({"a": [1]}).lazy()
    added, removed = row_differences(df1, df2)
    assert added == 0
    assert removed == 1


df_st = dataframes(cols=[column("a", dtype=pl.Int64), column("b", dtype=pl.Boolean)])


@given(df1=df_st, df2=df_st)
def test_row_differences_properties(df1: pl.DataFrame, df2: pl.DataFrame) -> None:
    added, removed = row_differences(df1.lazy(), df2.lazy())
    assert added >= 0, "added should be >= 0"
    assert added <= len(df2), "added should be <= len(df2)"
    assert removed >= 0, "removed should be >= 0"
    assert removed <= len(df1), "removed should be <= len(df1)"
    assert df1.height - removed + added == df2.height, "df1 - removed + added == df2"
    assert df2.height - added + removed == df1.height, "df2 - added + removed == df1"


def test_unique_row_differences():
    df1 = pl.DataFrame({"a": [1, 2, 3], "b": [False, False, False]}).lazy()
    df2 = pl.DataFrame({"a": [2, 3, 4], "b": [True, False, False]}).lazy()
    added, removed, updated = unique_row_differences(df1, df2, on="a")
    assert added == 1
    assert removed == 1
    assert updated == 1

    df1 = pl.DataFrame({"a": [1, 2, 3], "b": [False, False, False]}).lazy()
    df2 = pl.DataFrame({"a": [1, 2, 3], "b": [True, True, False]}).lazy()
    added, removed, updated = unique_row_differences(df1, df2, on="a")
    assert added == 0
    assert removed == 0
    assert updated == 2


df_st = dataframes(
    cols=[column("a", dtype=pl.Int64, unique=True), column("b", dtype=pl.Boolean)]
)


@given(df1=df_st, df2=df_st)
def test_unique_row_differences_properties(df1: pl.DataFrame, df2: pl.DataFrame):
    added, removed, updated = unique_row_differences(df1.lazy(), df2.lazy(), on="a")
    assert added >= 0, "added should be >= 0"
    assert added <= len(df2), "added should be <= len(df2)"
    assert removed >= 0, "removed should be >= 0"
    assert removed <= len(df1), "removed should be <= len(df1)"
    assert updated >= 0, "updated should be >= 0"
    assert updated <= len(df1), "updated should be <= len(df1)"
    assert df1.height - removed + added == df2.height, "df1 - removed + added == df2"
    assert df2.height - added + removed == df1.height, "df2 - added + removed == df1"


def test_parse_json():
    jsons = pl.Series(name="data", values=['{"a": 1}', '{"a": 2}', '{"b": 3}'])
    dtype = pl.Struct([pl.Field("a", pl.Int64), pl.Field("b", pl.Int64)])
    expected = pl.Series(
        name="data", values=[{"a": 1}, {"a": 2}, {"b": 3}], dtype=dtype
    )
    actual = parse_json(jsons, dtype=dtype)
    assert_series_equal(actual, expected)

    jsons = pl.Series(name="data", values=["[1, 2]", "[3, 4]", "[5, 6]"])
    dtype = pl.List(pl.Int64)
    expected = pl.Series(name="data", values=[[1, 2], [3, 4], [5, 6]], dtype=dtype)
    actual = parse_json(jsons, dtype=dtype)
    assert_series_equal(actual, expected)


def test_request_text():
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

    data = parse_json(texts)
    args = pl.Series(
        name="args",
        values=[{"foo": "1"}, {"foo": "2"}, {"foo": "3"}],
        dtype=pl.Struct([pl.Field("foo", pl.Utf8)]),
    )
    assert_series_equal(data.struct.field("args"), args)
