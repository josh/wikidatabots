import os

import requests
from tqdm import tqdm

from sparql import sparql

QUERY_BATCH_SIZE = os.environ.get("QUERY_BATCH_SIZE", "100")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")


def main():
    query = """
    SELECT ?item ?imdb ?random WHERE {
      VALUES ?classes {
        wd:Q11424
        wd:Q1261214
      }
      ?item wdt:P31/wdt:P279* ?classes .
      ?item wdt:P345 ?imdb.
      FILTER NOT EXISTS { ?item p:P4947 []. }
      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    """
    query += "LIMIT " + QUERY_BATCH_SIZE
    results = sparql(query)

    print("qid,P4947")
    for result in tqdm(results):
        tmdb_id = lookup_tmdb_movie_id(result["imdb"])
        if not tmdb_id:
            continue
        print('{},"""{}"""'.format(result["item"], tmdb_id))


def lookup_tmdb_movie_id(imdb_id, api_key=TMDB_API_KEY):
    params = {
        "api_key": api_key,
        "external_source": "imdb_id",
    }
    r = requests.get("https://api.themoviedb.org/3/find/" + imdb_id, params=params)
    r.raise_for_status()
    data = r.json()
    results = data.get("movie_results")
    if not results:
        return None
    return results[0]["id"]


if __name__ == "__main__":
    main()
