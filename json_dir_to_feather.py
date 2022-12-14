# pyright: basic

import os
import sys
from glob import glob
from io import StringIO

import pandas as pd

root_dir = sys.argv[1]
filenames = glob("*.json", root_dir=root_dir)

buf = StringIO()
for fn in filenames:
    with open(os.path.join(root_dir, fn)) as f:
        buf.write(f.read())
        buf.write("\n")
buf.seek(0)

df = pd.read_json(buf, lines=True)
df.info(buf=sys.stderr)
df.to_feather(sys.argv[2])
