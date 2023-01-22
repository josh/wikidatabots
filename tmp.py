import os

from polars_utils import read_ipc
from tmdb_etl import insert_tmdb_latest_changes

tmdb_type = os.environ["TYPE"]
df = read_ipc("latest_changes.arrow")
df = insert_tmdb_latest_changes(df, tmdb_type)
df.collect().write_ipc("latest_changes.arrow", compression="lz4")
