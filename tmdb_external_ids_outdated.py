import sys

import pandas as pd

df2 = (
    pd.read_feather(sys.argv[2], columns=["id", "date"])
    .drop_duplicates(subset=["id"], keep="last")
    .set_index("id")
    .sort_index()
)

df = pd.read_feather(sys.argv[1], columns=["retrieved_at"]).join(df2, how="left")
df = df[df["date"] >= df["retrieved_at"]]
df.to_csv(sys.stdout.buffer, columns=[], header=False)
