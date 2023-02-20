# pyright: strict

import logging

import polars as pl

from wd_tmdb import find_tmdb_ids_not_found


def main():
    df = pl.concat(
        [
            find_tmdb_ids_not_found("movie"),
            find_tmdb_ids_not_found("tv"),
            find_tmdb_ids_not_found("person"),
        ]
    )

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
