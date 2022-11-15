import numpy as np
import pyarrow as pa
import pyarrow.feather as feather
from pyarrow import json


def main(input_path: str, output_path: str):
    table = json.read_json(input_path)
    valid_ids = table["id"].to_numpy()
    size = valid_ids.max() + 1
    mask = np.ones(size, bool)
    mask[valid_ids] = 0
    bitmap = pa.Table.from_arrays([mask], names=["null"])
    feather.write_feather(bitmap, output_path)


if __name__ == "__main__":
    import sys

    main(sys.argv[1], sys.argv[2])
