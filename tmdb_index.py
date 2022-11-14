import logging
import sys

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.feather as feather
from pyarrow import json


def main():
    index_type = sys.argv[1]
    table = json.read_json(sys.argv[2])
    size: int = pc.max(table["id"]).as_py() + 1  # type: ignore
    mask = np.ones(size, bool)
    for id in table["id"]:
        mask[id.as_py()] = False
    bitmap = pa.Table.from_arrays([mask], names=["null"])
    feather.write_feather(bitmap, f"{index_type}-mask.feather")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
