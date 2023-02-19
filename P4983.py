# pyright: strict

import logging

import polars as pl

from wd_tmdb import find_tmdb_ids_via_imdb_id, find_tmdb_ids_via_tvdb_id

IMDB_QUERY = """
SELECT ?item ?imdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  VALUES ?classes {
    wd:Q15416
  }
  ?item (wdt:P31/(wdt:P279*)) ?classes.

  OPTIONAL { ?item p:P4983 ?tmdb_id. }
  FILTER(!(BOUND(?tmdb_id)))
}
"""

TVDB_QUERY = """
SELECT ?item ?tvdb_id WHERE {
  ?item wdt:P4835 ?tvdb_id.

  VALUES ?classes {
    wd:Q15416
  }
  ?item (wdt:P31/(wdt:P279*)) ?classes.

  FILTER(xsd:integer(?tvdb_id))

  OPTIONAL { ?item p:P4983 ?tmdb_id. }
  FILTER(!(BOUND(?tmdb_id)))
}
"""


def main() -> None:
    imdb_df = find_tmdb_ids_via_imdb_id(
        tmdb_type="tv",
        sparql_query=IMDB_QUERY,
        wd_pid="P4983",
        wd_plabel="TMDb TV series ID",
    )

    tvdb_df = find_tmdb_ids_via_tvdb_id(
        tmdb_type="tv",
        sparql_query=TVDB_QUERY,
        wd_pid="P4983",
        wd_plabel="TMDb TV series ID",
    )

    df = pl.concat([imdb_df, tvdb_df])

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
