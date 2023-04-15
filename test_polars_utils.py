# pyright: strict


from math import ceil
from typing import Iterator

import polars as pl
import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from polars.testing import assert_frame_equal
from polars.testing.parametric import column, dataframes, series

from polars_utils import (
    align_to_index,
    apply_with_tqdm,
    assert_called_once,
    assert_expression,
    compute_stats,
    drop_columns,
    expr_indicies_sorted,
    expr_repl,
    filter_columns,
    frame_diff,
    groups_of,
    is_constant,
    merge_with_indicator,
    outlier_exprs,
    read_xml,
    series_indicies,
    series_indicies_sorted,
    update_or_append,
    xml_extract,
)


def setup_module() -> None:
    pl.enable_string_cache(True)


def teardown_module() -> None:
    pl.enable_string_cache(False)


def test_assert_not_null():
    df = pl.LazyFrame({"a": []})
    ldf = df.pipe(assert_expression, pl.col("a").is_not_null())
    ldf.collect()

    df = pl.LazyFrame({"a": [1, 2, 3], "b": [1, None, 2]})

    ldf = df.pipe(assert_expression, pl.col("a").is_not_null())
    ldf.collect()

    ldf = df.pipe(assert_expression, pl.col("b").is_not_null())
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()

    ldf = df.pipe(assert_expression, pl.all().is_not_null())
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()


def test_merge_with_indicator() -> None:
    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 2, 3]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [2, 3, 4], "b": [3, 3, 4]}).map(assert_called_once())

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


def test_assert_unique():
    df = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 1, 2]})

    ldf = df.pipe(assert_expression, pl.col("a").drop_nulls().is_unique())
    ldf.collect()

    ldf = df.pipe(assert_expression, pl.col("b").drop_nulls().is_unique())
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()

    ldf = df.pipe(assert_expression, pl.all().drop_nulls().is_unique())
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()


def test_assert_count() -> None:
    df = pl.LazyFrame({"a": [1, 2, 3], "b": [1, 1, 2]})

    ldf = df.pipe(assert_expression, pl.count() <= 3)
    ldf.collect()

    ldf = df.pipe(assert_expression, pl.count() <= 4)
    ldf.collect()

    ldf = df.pipe(assert_expression, pl.count() < 5)
    ldf.collect()

    ldf = df.pipe(assert_expression, pl.count() <= 2)
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()

    ldf = df.pipe(assert_expression, pl.count() <= 1)
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()

    ldf = df.pipe(assert_expression, pl.count() > 0)
    ldf.collect()

    df = pl.LazyFrame({"a": []})
    ldf = df.pipe(assert_expression, pl.count() > 0)
    with pytest.raises(pl.ComputeError):  # type: ignore
        ldf.collect()


def test_expr_repl() -> None:
    assert expr_repl(pl.col("a")) == 'pl.col("a")'
    assert expr_repl(pl.col("a").alias("b")) == 'pl.col("a").alias("b")'
    assert expr_repl(pl.col("a").alias("b"), strip_alias=True) == 'pl.col("a")'

    assert expr_repl(pl.col("a").is_not_null()) == 'pl.col("a").is_not_null()'
    assert (
        expr_repl(pl.col("a").is_not_null().alias("b"))
        == 'pl.col("a").is_not_null().alias("b")'
    )
    assert (
        expr_repl(pl.col("a").alias("b").is_not_null())
        == 'pl.col("a").alias("b").is_not_null()'
    )
    assert (
        expr_repl(pl.col("a").is_not_null().alias("b"), strip_alias=True)
        == 'pl.col("a").is_not_null()'
    )
    assert (
        expr_repl(pl.col("a").alias("b").is_not_null(), strip_alias=True)
        == 'pl.col("a").is_not_null()'
    )


@given(
    df=dataframes(max_cols=5, max_size=20),
    predicate=st.one_of(
        [
            st.just(pl.all().is_duplicated().all()),
            st.just(pl.all().is_duplicated().any()),
            st.just(pl.all().is_null().all()),
            st.just(pl.all().is_null().any()),
            st.just(pl.all().null_count() > 0),
            st.just(pl.all().null_count() > 10),
        ]
    ),
)
def test_filter_drop_columns_properties(df: pl.DataFrame, predicate: pl.Expr) -> None:
    df2 = filter_columns(df, predicate)
    assert set(df2.columns).issubset(set(df.columns))

    df2 = drop_columns(df, predicate)
    assert set(df2.columns).issubset(set(df.columns))


def test_is_constant() -> None:
    df = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [3, 3, 3],
            "c": [2, 2, None],
            "d": [True, True, False],
            "e": [True, True, True],
            "f": [False, False, False],
            "g": [True, True, None],
            "h": [None, None, None],
        }
    )
    df2 = pl.DataFrame(
        {
            "a": [False],
            "b": [True],
            "c": [False],
            "d": [False],
            "e": [True],
            "f": [True],
            "g": [False],
            "h": [True],
        }
    )
    assert_frame_equal(df.select(pl.all().pipe(is_constant)), df2)


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


def test_xml_extract() -> None:
    dtype = pl.List(pl.Struct({"name": pl.Utf8, "year": pl.Int64}))

    df = (
        pl.DataFrame({"xml": [XML_EXAMPLE]})
        .select(
            pl.col("xml").pipe(xml_extract, dtype).alias("country"),
        )
        .explode("country")
        .select(
            pl.col("country").struct.field("name").alias("name"),
            pl.col("country").struct.field("year").alias("year"),
        )
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
    ).map(assert_called_once())
    df2 = pl.LazyFrame(
        {
            "id": pl.Series([0, 1, 2, 3, 4, 5], dtype=pl.Int8),
            "value": [None, 1, 2, None, None, 5],
        }
    ).map(assert_called_once())
    assert_frame_equal(align_to_index(df1, name="id"), df2)

    df1 = pl.LazyFrame(
        {
            "id": pl.Series([255], dtype=pl.UInt8),
            "value": [42],
        }
    ).map(assert_called_once())
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
    with pytest.raises(AssertionError):
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


def test_update_or_append() -> None:
    df1 = pl.LazyFrame({"a": []}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": []}).map(assert_called_once())
    df3 = pl.LazyFrame({"a": []})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [1]}).map(assert_called_once())
    df3 = pl.LazyFrame({"a": [1]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [2]}).map(assert_called_once())
    df3 = pl.LazyFrame({"a": [1, 2]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1], "b": [True]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [2]}).map(assert_called_once())
    df3 = pl.LazyFrame({"a": [1, 2], "b": [True, None]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1], "b": [True]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [2], "b": [False]}).map(assert_called_once())
    df3 = pl.LazyFrame({"a": [1, 2], "b": [True, False]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1, 2], "b": [True, True]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [3], "b": [False]}).map(assert_called_once())
    df3 = pl.LazyFrame({"a": [1, 2, 3], "b": [True, True, False]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1], "b": [1], "c": [True]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [1], "b": [2]}).map(assert_called_once())
    df3 = pl.LazyFrame({"a": [1], "b": [2], "c": [True]})
    assert_frame_equal(update_or_append(df1, df2, on="a"), df3)

    df1 = pl.LazyFrame({"a": [1], "b": [1], "c": [True]}).map(assert_called_once())
    df2 = pl.LazyFrame({"a": [1], "b": [2], "c": [False]}).map(assert_called_once())
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
    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": [False, False, False]}).map(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [2, 3, 4], "b": [True, False, False]}).map(
        assert_called_once()
    )
    added, removed, updated, b_updated = frame_diff(df1, df2, on="a").collect().row(0)
    assert added == 1
    assert removed == 1
    assert updated == 1
    assert b_updated == 1

    df1 = pl.LazyFrame({"a": [1, 2, 3], "b": [False, False, False]}).map(
        assert_called_once()
    )
    df2 = pl.LazyFrame({"a": [1, 2, 3], "b": [True, True, False]}).map(
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
        df1.lazy().map(assert_called_once()),
        df2.lazy().map(assert_called_once()),
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
    df=dataframes(
        cols=[
            column("a", dtype=pl.Int64),
            column("b", dtype=pl.Boolean),
            column("c", dtype=pl.Boolean, null_probability=0.5),
            column("d", dtype=pl.Boolean, null_probability=0.1),
            column("e", dtype=pl.Boolean, null_probability=0.01),
        ]
    )
)
def test_outlier_exprs(df: pl.DataFrame) -> None:
    outlier_exprs(
        df,
        [
            pl.col("a").is_not_null().alias("a_not_null"),
            (pl.col("a") < pl.col("a").mean()).alias("a_lt_mean"),
            pl.col("b"),
            pl.col("c"),
            pl.col("d"),
            pl.col("e"),
        ],
    )


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


@given(
    a=st.lists(st.integers(min_value=0, max_value=10), min_size=0, max_size=10),
    b=st.lists(st.integers(min_value=0, max_value=10), min_size=1, max_size=10),
)
def test_indices(a: list[int], b: list[int]) -> None:
    c = list(_lst_indicies(a, b))

    a_s = pl.Series(a, dtype=pl.UInt32)
    b_s = pl.Series(b, dtype=pl.UInt32)

    s = series_indicies(a_s, b_s)
    assert s.dtype == pl.UInt32
    assert len(s) == len(a)
    assert c == s.to_list()


@given(
    a=st.lists(st.integers(min_value=0, max_value=10), min_size=0, max_size=10),
    b=st.lists(st.integers(min_value=0, max_value=10), min_size=1, max_size=10),
)
def test_indices_sorted(a: list[int], b: list[int]) -> None:
    b = sorted(b)
    c = list(_lst_indicies(a, b))

    a_s = pl.Series(a, dtype=pl.UInt32)
    b_s = pl.Series(b, dtype=pl.UInt32)

    s = series_indicies(a_s, b_s)
    assert s.dtype == pl.UInt32
    assert len(s) == len(a)
    assert c == s.to_list()

    s = series_indicies_sorted(a_s, b_s)
    assert s.dtype == pl.UInt32
    assert len(s) == len(a)
    assert c == s.to_list()

    s = pl.select(expr_indicies_sorted(pl.lit(a_s), pl.lit(b_s))).to_series()
    assert s.dtype == pl.UInt32
    assert len(s) == len(a)
    assert c == s.to_list()


@given(
    df=dataframes(max_cols=20, max_size=10, null_probability=0.1),
)
def test_compute_stats(df: pl.DataFrame) -> None:
    stats_df = compute_stats(df)
    assert len(stats_df) == len(df.columns)
