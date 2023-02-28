# pyright: strict

from typing import TypeVar

import polars as pl
import pytest
from hypothesis import given
from polars.testing import assert_frame_equal
from polars.testing.parametric import column, dataframes

from polars_utils import (
    align_to_index,
    apply_with_tqdm,
    assert_not_null,
    assert_unique,
    read_xml,
    row_differences,
    unique_row_differences,
    update_ipc,
    xml_to_dtype,
)


def test_assert_not_null():
    df = pl.LazyFrame({"a": [1, 2, 3], "b": [1, None, 2]})

    ldf = df.pipe(assert_not_null, pl.col("a"))
    ldf.collect()

    ldf = df.pipe(assert_not_null, pl.col("b"))
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()

    ldf = df.pipe(assert_not_null, pl.all())
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()


def test_assert_unique():
    df = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 1, 2]})

    ldf = df.pipe(assert_unique, pl.col("a"))
    ldf.collect()

    ldf = df.pipe(assert_unique, pl.col("b"))
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()

    ldf = df.pipe(assert_unique, pl.all())
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()


def test_update_ipc():
    filename = "/tmp/test_polars_utils.arrow"
    df = pl.DataFrame({"a": [1, 2, 3]}).write_ipc(filename)

    update_ipc(filename, lambda df: df.with_columns(pl.col("a") * 2))

    df = pl.read_ipc(filename, memory_map=False)
    df2 = pl.DataFrame({"a": [2, 4, 6]})
    assert_frame_equal(df, df2)


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


def test_read_xml():
    schema: dict[str, pl.PolarsDataType] = {
        "name": pl.Utf8,
        "rank": pl.Int64,
        "year": pl.Int64,
        "gdppc": pl.Int64,
        "neighbor": pl.List(
            pl.Struct(
                [
                    pl.Field("name", pl.Utf8),
                    pl.Field("direction", pl.Utf8),
                ]
            )
        ),
    }

    df = pl.DataFrame(
        {
            "name": ["Liechtenstein", "Singapore", "Panama"],
            "rank": [1, 4, 68],
            "year": [2008, 2011, 2011],
            "gdppc": [141100, 59900, 13600],
            "neighbor": [
                [
                    {"name": "Austria", "direction": "E"},
                    {"name": "Switzerland", "direction": "W"},
                ],
                [{"name": "Malaysia", "direction": "N"}],
                [
                    {"name": "Costa Rica", "direction": "W"},
                    {"name": "Colombia", "direction": "E"},
                ],
            ],
        },
        schema=schema,
    )

    assert_frame_equal(read_xml(XML_EXAMPLE, schema=schema), df)


def test_read_xml_with_overrides():
    schema: dict[str, pl.PolarsDataType] = {
        "name": pl.Utf8,
        "rank": pl.Int64,
        "neighbor": pl.List(pl.Struct({"name": pl.Utf8})),
    }

    df = pl.DataFrame(
        {
            "name": ["Liechtenstein", "Singapore", "Panama"],
            "rank": [1, 4, 68],
            "neighbor": [
                [
                    {"name": "Austria"},
                    {"name": "Switzerland"},
                ],
                [{"name": "Malaysia"}],
                [
                    {"name": "Costa Rica"},
                    {"name": "Colombia"},
                ],
            ],
        },
        schema=schema,
    )

    assert_frame_equal(read_xml(XML_EXAMPLE, schema=schema), df)


def test_read_xml_with_missing():
    schema: dict[str, pl.PolarsDataType] = {
        "name": pl.Utf8,
        "price": pl.UInt32,
    }
    df = pl.DataFrame(
        {
            "name": ["Liechtenstein", "Singapore", "Panama"],
            "price": [None, None, None],
        },
        schema=schema,
    )
    assert_frame_equal(read_xml(XML_EXAMPLE, schema=schema), df)

    df = pl.DataFrame({"name": [], "price": []}, schema=schema)
    assert_frame_equal(read_xml(XML_EXAMPLE, schema=schema, xpath="./foo"), df)


def test_xml_to_dtype():
    dtype = pl.List(
        pl.Struct(
            {
                "name": pl.Utf8,
                "rank": pl.Int64,
                "neighbor": pl.List(pl.Struct({"name": pl.Utf8})),
            }
        )
    )
    obj = [
        {
            "name": "Liechtenstein",
            "rank": 1,
            "neighbor": [{"name": "Austria"}, {"name": "Switzerland"}],
        },
        {
            "name": "Singapore",
            "rank": 4,
            "neighbor": [{"name": "Malaysia"}],
        },
        {
            "name": "Panama",
            "rank": 68,
            "neighbor": [{"name": "Costa Rica"}, {"name": "Colombia"}],
        },
    ]
    assert xml_to_dtype(XML_EXAMPLE, dtype=dtype) == obj


def test_align_to_index():
    df1 = pl.DataFrame([], schema={"id": pl.Int64}).lazy()
    assert_frame_equal(align_to_index(df1, name="id"), df1)

    df1 = pl.LazyFrame(
        {
            "id": pl.Series([1, 2, 5], dtype=pl.Int8),
            "value": [1, 2, 5],
        }
    )
    df2 = pl.LazyFrame(
        {
            "id": pl.Series([0, 1, 2, 3, 4, 5], dtype=pl.Int8),
            "value": [None, 1, 2, None, None, 5],
        }
    )
    assert_frame_equal(align_to_index(df1, name="id"), df2)

    df1 = pl.LazyFrame(
        {
            "id": pl.Series([255], dtype=pl.UInt8),
            "value": [42],
        }
    )
    df2 = align_to_index(df1, name="id").collect()
    assert df2.schema == {"id": pl.UInt8, "value": pl.Int64}
    assert df2.height == 256

    # df = pl.LazyFrame(
    #     {
    #         "id": [-1, 2, 5],
    #         "value": [-1, 2, 5],
    #     }
    # )
    # align_to_index(df, name="id").collect()

    df = pl.LazyFrame(
        {
            "id": ["a", "b", "c"],
            "value": [1, 2, 5],
        }
    )
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

    df2 = align_to_index(df.lazy(), name="b").collect()
    assert df2.height >= df.height


def test_align_to_index_evaluates_df_once():
    ldf1 = pl.LazyFrame(
        {
            "id": pl.Series([1, 2, 5], dtype=pl.Int8),
            "value": [1, 2, 5],
        }
    ).map(assert_called_once())
    align_to_index(ldf1, name="id").collect()

    ldf2 = pl.LazyFrame(
        {
            "id": pl.Series([1, 2, 5], dtype=pl.Int8),
            "value": [1, 2, 5],
        }
    ).select(
        [
            pl.col("id").map(assert_called_once(), return_dtype=pl.Int8),
            pl.col("value").map(assert_called_once(), return_dtype=pl.Int64),
        ]
    )
    align_to_index(ldf2, name="id").collect()


def test_row_differences():
    df1 = pl.LazyFrame({"a": [1, 2, 3]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [2, 3, 4]}).map(assert_called_once())
    added, removed = row_differences(df1, df2)
    assert added == 1
    assert removed == 1

    df1 = pl.LazyFrame({"a": [1]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [1, 2, 3, 4]}).map(assert_called_once())
    added, removed = row_differences(df1, df2)
    assert added == 3
    assert removed == 0

    df1 = pl.LazyFrame({"a": [1, 2, 3, 4]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [1]}).map(assert_called_once())
    added, removed = row_differences(df1, df2)
    assert added == 0
    assert removed == 3

    df1 = pl.LazyFrame({"a": [1]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [1, 1]}).map(assert_called_once())
    added, removed = row_differences(df1, df2)
    assert added == 1
    assert removed == 0

    df1 = pl.LazyFrame({"a": [1, 1]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [1]}).map(assert_called_once())
    added, removed = row_differences(df1, df2)
    assert added == 0
    assert removed == 1


df_st = dataframes(cols=[column("a", dtype=pl.Int64), column("b", dtype=pl.Boolean)])


@given(df1=df_st, df2=df_st)
def test_row_differences_properties(df1: pl.DataFrame, df2: pl.DataFrame) -> None:
    added, removed = row_differences(
        df1.lazy().map(assert_called_once()),
        df2.lazy().map(assert_called_once()),
    )
    assert added >= 0, "added should be >= 0"
    assert added <= len(df2), "added should be <= len(df2)"
    assert removed >= 0, "removed should be >= 0"
    assert removed <= len(df1), "removed should be <= len(df1)"
    assert df1.height - removed + added == df2.height, "df1 - removed + added == df2"
    assert df2.height - added + removed == df1.height, "df2 - added + removed == df1"


def test_unique_row_differences():
    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": [False, False, False]}).map(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [2, 3, 4], "b": [True, False, False]}).map(
        assert_called_once()
    )
    added, removed, updated = unique_row_differences(df1, df2, on="a")
    assert added == 1
    assert removed == 1
    assert updated == 1

    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": [False, False, False]}).map(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [1, 2, 3], "b": [True, True, False]}).map(
        assert_called_once()
    )
    added, removed, updated = unique_row_differences(df1, df2, on="a")
    assert added == 0
    assert removed == 0
    assert updated == 2


df_st = dataframes(
    cols=[column("a", dtype=pl.Int64, unique=True), column("b", dtype=pl.Boolean)]
)


@given(df1=df_st, df2=df_st)
def test_unique_row_differences_properties(df1: pl.DataFrame, df2: pl.DataFrame):
    added, removed, updated = unique_row_differences(
        df1.lazy().map(assert_called_once()),
        df2.lazy().map(assert_called_once()),
        on="a",
    )
    assert added >= 0, "added should be >= 0"
    assert added <= len(df2), "added should be <= len(df2)"
    assert removed >= 0, "removed should be >= 0"
    assert removed <= len(df1), "removed should be <= len(df1)"
    assert updated >= 0, "updated should be >= 0"
    assert updated <= len(df1), "updated should be <= len(df1)"
    assert df1.height - removed + added == df2.height, "df1 - removed + added == df2"
    assert df2.height - added + removed == df1.height, "df2 - added + removed == df1"


def test_apply_with_tqdm():
    df1 = pl.LazyFrame({"s": [1, 2, 3]})
    df2 = pl.LazyFrame({"s": [2, 3, 4]})
    df3 = df1.select(
        apply_with_tqdm(
            pl.col("s"),
            lambda x: x + 1,
            return_dtype=pl.Int64,
        )
    )
    assert df3.schema == {"s": pl.Int64}
    assert_frame_equal(df2, df3)


T = TypeVar("T")


def assert_called_once():
    calls: int = 1

    def mock(value: T) -> T:
        nonlocal calls
        calls -= 1
        assert calls >= 0, "mock called too many times"
        return value

    return mock
