# pyright: basic

import logging
import atexit
import time

import pyarrow as pa
import pyarrow.feather as feather
from pyarrow import fs

import imdb
import tmdb

CACHED_INDEXES: dict[tmdb.ObjectType, pa.Table] = {}

lookup_count = 0
filter_hit_count = 0


def load_index(tmdb_type: tmdb.ObjectType) -> pa.Table:
    if tmdb_type in CACHED_INDEXES:
        return CACHED_INDEXES[tmdb_type]

    start = time.time()
    imdb_type = tmdb.TMDB_TYPE_TO_IMDB_TYPE[tmdb_type]
    s3 = fs.S3FileSystem(region="us-east-1")
    f = s3.open_input_file(
        f"wikidatabots/imdb/{imdb_type}/tmdb_{tmdb_type}_exists.arrow"
    )
    table = feather.read_table(f)
    CACHED_INDEXES[tmdb_type] = table
    elapsed_seconds = time.time() - start
    logging.info(f"Loaded {tmdb_type} external_ids feather index in {elapsed_seconds}s")
    return table


def fast_imdb_id_lookup(tmdb_type: tmdb.ObjectType, imdb_id: imdb.ID) -> bool:
    global lookup_count
    global filter_hit_count

    imdb_type = tmdb.TMDB_TYPE_TO_IMDB_TYPE[tmdb_type]

    numeric_id = imdb.decode_numeric_id(imdb_id, imdb_type)
    if not numeric_id:
        return False

    lookup_count += 1
    table = load_index(tmdb_type)

    tmdb_id: int = table["tmdb_exists"][numeric_id].as_py()
    if tmdb_id == 0:
        filter_hit_count += 1
    return tmdb_id == 1


def log_stats():
    if lookup_count:
        percentage = (filter_hit_count / lookup_count) * 100
        filter_miss_count = lookup_count - filter_hit_count
        logging.info(
            "tmdb external_ids filter efficiency: "
            f"{percentage}%, {filter_miss_count} misses"
        )


atexit.register(log_stats)
