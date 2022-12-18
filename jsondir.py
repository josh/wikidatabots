# pyright: basic

import json
import os
import sys
from glob import glob
from typing import Iterator

import pandas as pd


def read_json_dir(dirname: str) -> Iterator[dict]:
    for basename in sorted(glob("*.json", root_dir=dirname)):
        filename = os.path.join(dirname, basename)
        with open(filename) as f:
            data = json.load(f)
            assert type(data) == dict
            assert "filename" not in data
            data["filename"] = basename.removesuffix(".json")
            yield data


def read_json_dir_as_df(dirname: str, **kargs) -> pd.DataFrame:
    return pd.DataFrame(read_json_dir(dirname), **kargs)


if __name__ == "__main__":
    df = read_json_dir_as_df(dirname=sys.argv[1])
    df.info(buf=sys.stderr)
    df.to_feather(sys.argv[2])
