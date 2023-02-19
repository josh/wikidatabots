# pyright: strict

import logging

from wd_tmdb import find_tmdb_ids_via_imdb_id

QUERY = """
SELECT ?item ?imdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  VALUES ?classes {
    wd:Q11424
    wd:Q1261214
  }
  ?item (wdt:P31/(wdt:P279*)) ?classes.

  OPTIONAL { ?item wdt:P4947 ?tmdb_id. }
  FILTER(!(BOUND(?tmdb_id)))
}
"""


def main() -> None:
    df = find_tmdb_ids_via_imdb_id(
        tmdb_type="movie",
        sparql_query=QUERY,
        wd_pid="P4947",
        wd_plabel="TMDb movie ID",
    )

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
