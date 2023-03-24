# pyright: strict

import sys

import polars as pl


def main() -> None:
    pl.read_ipc(sys.argv[1], memory_map=False).write_parquet(sys.argv[2])


if __name__ == "__main__":
    main()
