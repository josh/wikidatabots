# pyright: strict

import logging

from wd_tmdb import find_tmdb_ids_via_imdb_id

QUERY = """
SELECT ?item ?imdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  ?item wdt:P31 wd:Q5.

  OPTIONAL { ?item wdt:P4985 ?tmdb_id. }
  FILTER(!(BOUND(?tmdb_id)))
}
"""


def main() -> None:
    df = find_tmdb_ids_via_imdb_id(tmdb_type="person", sparql_query=QUERY)

    for (line,) in df.collect().iter_rows():
        print(line)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
