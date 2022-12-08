# pyright: basic

import os
import sys

import numpy as np
import numpy.ma as ma
import pyarrow as pa
import pyarrow.feather as feather

from utils import ma_reserve_capacity

main_filename = sys.argv[1]
changes_filename = sys.argv[2]

table = feather.read_table(main_filename)
changed_at = ma.masked_array(
    table["changed_at"].to_numpy(),
    mask=table["changed_at"].is_null().to_numpy(),
)

changes_table = feather.read_table(changes_filename)
changed_ids = changes_table["id"].to_numpy()
datestr = os.path.basename(changes_filename).removesuffix(".arrow")
date = np.datetime64(datestr, "D")

changed_at = ma_reserve_capacity(changed_at, changed_ids.max() + 1)

updated_rows = 0

for id in changed_ids:
    if changed_at[id] is ma.masked or changed_at[id] < date:
        changed_at[id] = date
        updated_rows += 1

if updated_rows:
    print(f"{datestr}: updated rows: {updated_rows}", file=sys.stderr)
    table = pa.table([changed_at], names=["changed_at"])
    feather.write_feather(table, main_filename)
else:
    print(f"{datestr}: no rows updated", file=sys.stderr)
