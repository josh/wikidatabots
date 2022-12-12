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
changed_at_col = ma.masked_array(
    table["changed_at"].to_numpy(),
    mask=table["changed_at"].is_null().to_numpy(),
)
adult_col = ma.masked_array(
    table["adult"].to_numpy(),
    mask=table["adult"].is_null().to_numpy(),
)

changes_table = feather.read_table(changes_filename)
changed_ids = changes_table["id"].to_numpy()
changed_adult = changes_table["adult"].to_numpy()
datestr = os.path.basename(changes_filename).removesuffix(".arrow")
date = np.datetime64(datestr, "D")

new_size = changed_ids.max() + 1
changed_at_col = ma_reserve_capacity(changed_at_col, new_size)
adult_col = ma_reserve_capacity(adult_col, new_size)
id_col = pa.array(range(len(changed_at_col)), type=pa.int64())

updated_rows = 0

for idx, id in enumerate(changed_ids):
    if changed_at_col[id] is ma.masked or changed_at_col[id] < date:
        changed_at_col[id] = date
        adult_col[id] = changed_adult[idx]
        updated_rows += 1

if updated_rows:
    table = pa.table(
        [id_col, changed_at_col, adult_col],
        names=["id", "changed_at", "adult"],
    )
    feather.write_feather(table, main_filename)
else:
    print(f"{datestr}: no rows updated", file=sys.stderr)
