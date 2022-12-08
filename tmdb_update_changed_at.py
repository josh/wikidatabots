# pyright: basic

import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.feather as feather

from utils import np_reserve_capacity

main_filename = sys.argv[1]
changes_filename = sys.argv[2]

table = feather.read_table(main_filename)
all_timestamps = table["changed_at"].to_numpy().copy()

changes_table = feather.read_table(changes_filename)
changed_ids = changes_table["id"].to_numpy()
datestr = os.path.basename(changes_filename).removesuffix(".arrow")
date = np.datetime64(datestr, "D")

size = changed_ids.max() + 1
all_timestamps = np_reserve_capacity(all_timestamps, size, np.datetime64("nat"))

updated_rows = 0

for id in changed_ids:
    if all_timestamps[id] < date:
        all_timestamps[id] = date
        updated_rows += 1

if updated_rows:
    print(f"{datestr}: updated rows: {updated_rows}", file=sys.stderr)
    table = pa.table([all_timestamps], names=["changed_at"])
    feather.write_feather(table, main_filename)
else:
    print(f"{datestr}: no rows updated", file=sys.stderr)
