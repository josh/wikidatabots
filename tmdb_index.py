import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.feather as feather
from pyarrow import json


def main(input_path: str, output_path: str):
    table = json.read_json(input_path)
    size: int = pc.max(table["id"]).as_py() + 1  # type: ignore
    mask = np.ones(size, bool)
    for id in table["id"]:
        mask[id.as_py()] = False
    bitmap = pa.Table.from_arrays([mask], names=["null"])
    feather.write_feather(bitmap, output_path)


if __name__ == "__main__":
    import sys

    main(sys.argv[1], sys.argv[2])
