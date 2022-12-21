import sys

import pandas as pd

df = pd.read_feather(sys.argv[1])
df = df[df["success"].isna()]
df.head(10000).to_csv(sys.stdout.buffer, columns=[], header=False)
