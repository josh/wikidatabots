# pyright: basic

import logging

import numpy as np
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq
from pyarrow import json


def main(input_path: str, output_path: str, output_path2: str):
    table = json.read_json(input_path)
    valid_ids = table["id"].to_numpy()
    size = valid_ids.max() + 1
    mask = np.ones(size, bool)
    mask[valid_ids] = 0
    null_count = np.sum(mask)
    logging.info(f"Null count: {null_count}/{size}")
    bitmap = pa.Table.from_arrays([mask], names=["null"])
    feather.write_feather(bitmap, output_path)
    pq.write_table(bitmap, output_path2)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    main(sys.argv[1], sys.argv[2], sys.argv[3])
