# pyright: basic

import argparse

import pandas as pd

parser = argparse.ArgumentParser(description="Insert or update data frame")
parser.add_argument("df_a", action="store")
parser.add_argument("df_b", action="store")
parser.add_argument("--key", action="store", default="id")
args = parser.parse_args()

df_a = pd.read_feather(args.df_a)
df_b = pd.read_feather(args.df_b)
df_c = pd.concat([df_a, df_b])
df_c = df_c.drop_duplicates(args.key, keep="last")
df_c = df_c.sort_values(args.key)
df_c.reset_index(drop=True).to_feather(args.df_a)
