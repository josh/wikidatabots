# pyright: basic

import sys

import pandas as pd

from jsondir import read_json_dir_as_df

df = input_df = pd.read_feather(sys.argv[1])
df = df.set_index("id")

changed_df = (
    read_json_dir_as_df(sys.argv[2])
    .rename(columns={"filename": "id", "id": "id_"})
    .astype({"id": int})
    .set_index("id")
)

changed_df["imdb_numeric_id"] = pd.to_numeric(
    changed_df["imdb_id"].str.removeprefix("tt").str.removeprefix("nm"),
    errors="coerce",
).astype("Int64")
changed_df["retrieved_at"] = pd.Timestamp.now().floor("s")
changed_df = changed_df[df.columns]

print(changed_df, file=sys.stderr)

df = pd.concat([df[~df.index.isin(changed_df.index)], changed_df])
df = df.sort_index().reset_index(names=["id"])

assert (
    df.columns.tolist() == input_df.columns.tolist()
), f"{df.columns} != {input_df.columns}"
assert (
    df.dtypes.tolist() == input_df.dtypes.tolist()
), f"{df.dtypes} != {input_df.dtypes}"
df.to_feather(sys.argv[1])
