# pyright: basic

import sys

import pyarrow.feather as feather
from pyarrow import json

if len(sys.argv) > 1:
    table = json.read_json(sys.argv[1])
else:
    table = json.read_json(sys.stdin.buffer)

if len(sys.argv) > 2:
    feather.write_feather(table, sys.argv[2])
else:
    feather.write_feather(table, sys.stdout.buffer)
