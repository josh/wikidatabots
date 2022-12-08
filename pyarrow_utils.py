# pyright: basic

import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.feather as feather
from pyarrow import json


def fill_ids(table: pa.Table, key: str = "id") -> pa.Table:
    index = table[key]
    size = pc.max(index).as_py() + 1  # type: ignore
    filled_index = pa.array(range(size), type=index.type)
    filled_table = pa.table([filled_index], names=[key])
    return table.join(filled_table, key, join_type="right outer").sort_by(key)


def read_feather(filename: str) -> pa.Table:
    if filename == "-" or filename == "/dev/stdin":
        return feather.read_table(sys.stdin.buffer)
    else:
        assert filename.endswith(".arrow")
        return feather.read_table(filename)


def read_json(filename: str) -> pa.Table:
    if filename == "-" or filename == "/dev/stdin":
        return json.read_json(sys.stdin.buffer)
    else:
        assert filename.endswith(".json") or filename.endswith(".json.gz")
        return json.read_json(filename)


def read_table(filename: str, json: bool = False) -> pa.Table:
    if json or filename.endswith(".json") or filename.endswith(".json.gz"):
        return read_json(filename)
    else:
        return read_feather(filename)


def write_feather(table: pa.Table, filename: str) -> None:
    if filename == "-" or filename == "/dev/stdout":
        feather.write_feather(table, sys.stdout.buffer)
    else:
        assert filename.endswith(".arrow")
        feather.write_feather(table, filename)
