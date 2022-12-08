# pyright: basic

import pyarrow as pa

from pyarrow_utils import fill_ids


def test_fill_ids():
    table1 = pa.table({"id": [1, 3, 5], "name": ["a", "b", "c"]})
    table2 = fill_ids(table1, key="id")
    assert table2["id"].to_pylist() == [0, 1, 2, 3, 4, 5]
    assert table2["name"].to_pylist() == [None, "a", None, "b", None, "c"]

    table1 = pa.table({"id": [0, 1, 3], "name": ["a", "b", "c"]})
    table2 = fill_ids(table1, key="id")
    assert table2["id"].to_pylist() == [0, 1, 2, 3]
    assert table2["name"].to_pylist() == ["a", "b", None, "c"]
