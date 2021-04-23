from tqdm import tqdm

import tmdb
from sparql import sparql


def main():
    query = """
    SELECT DISTINCT ?item ?imdb ?random WHERE {
      ?item wdt:P345 ?imdb.

      VALUES ?classes {
        wd:Q11424
        wd:Q1261214
      }
      ?item (wdt:P31/(wdt:P279*)) ?classes.

      OPTIONAL { ?item wdt:P4947 ?tmdb. }
      FILTER(!(BOUND(?tmdb)))

      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    LIMIT 1000
    """
    results = sparql(query)

    print("qid,P4947")
    for result in tqdm(results):
        tmdb_id = tmdb.find_by_imdb_id(result["imdb"], type="movie")
        if not tmdb_id:
            continue
        print('{},"""{}"""'.format(result["item"], tmdb_id))


if __name__ == "__main__":
    main()
