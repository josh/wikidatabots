# pyright: basic

import pyarrow as pa
import pyarrow.compute as pc


def fill_ids(table: pa.Table, key: str = "id") -> pa.Table:
    index = table[key]
    size = pc.max(index).as_py() + 1  # type: ignore
    filled_index = pa.array(range(size), type=index.type)
    filled_table = pa.table([filled_index], names=[key])
    return table.join(filled_table, key, join_type="right outer").sort_by(key)
