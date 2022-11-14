import logging
import sys
from datetime import datetime
from typing import Literal

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.feather as feather
from pyarrow import fs, json

S3 = fs.S3FileSystem(region="us-east-1")
BUCKET_NAME = "wikidatabots"


def upload_table(table: pa.Table, path: str) -> None:
    logging.info(f"Uploading table to s3://{BUCKET_NAME}/{path}")
    f = S3.open_output_stream(f"{BUCKET_NAME}/{path}")
    feather.write_feather(table, f)


def fetch_table(path: str) -> pa.Table:
    logging.info(f"Fetching table from s3://{BUCKET_NAME}/{path}")
    f = S3.open_input_file(f"{BUCKET_NAME}/{path}")
    return feather.read_table(f)


TODAY_STR: str = datetime.utcnow().strftime("%m_%d_%Y")

IndexType = Literal["movie", "tv", "person"]
ExportName = Literal["movie_ids", "tv_series_ids", "person_ids"]

STR_TO_TYPE: dict[str, IndexType] = {
    "movie": "movie",
    "tv": "tv",
    "person": "person",
}

TYPE_TO_EXPORT_NAME: dict[IndexType, ExportName] = {
    "movie": "movie_ids",
    "tv": "tv_series_ids",
    "person": "person_ids",
}


def index_null_ids(table: pa.Table) -> pa.Table:
    size: int = pc.max(table["id"]).as_py() + 1  # type: ignore
    mask = np.ones(size, bool)
    for id in table["id"]:
        mask[id.as_py()] = False
    table = pa.Table.from_arrays([mask], names=["null"])
    return table


def upload_mask(type: IndexType, table: pa.Table) -> None:
    upload_table(table, f"tmdb/{type}/mask.feather")


def fetch_mask(type: IndexType) -> npt.NDArray[np.bool_]:
    return fetch_table(f"tmdb/{type}/mask.feather")[0].to_numpy()


def main():
    index_type = STR_TO_TYPE[sys.argv[1]]
    table = json.read_json(sys.argv[2])
    bitmap = index_null_ids(table)
    feather.write_feather(bitmap, f"{index_type}-mask.feather")
    upload_mask(index_type, bitmap)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
