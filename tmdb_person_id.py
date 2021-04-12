import os

import requests
from tqdm import tqdm

from sparql import sparql

QUERY_BATCH_SIZE = os.environ.get("QUERY_BATCH_SIZE", "100")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")

CLASSES_QUERY = """
SELECT ?c WHERE {
  wd:P4985 p:P2302 ?s.
  ?s ps:P2302 wd:Q21503250.
  ?s pq:P2308 ?c.
}
"""
CLASSES = {"Q5", "Q16334295", "Q95074", "Q14514600", "Q431289", "Q59755569"}


def main():
    assert {r["c"] for r in sparql(CLASSES_QUERY)} == CLASSES

    query = """
    SELECT ?item ?imdb ?random WHERE {
      ?item wdt:P345 ?imdb.

      VALUES ?classes {
        wd:Q5
        wd:Q16334295
        wd:Q95074
        wd:Q14514600
        wd:Q431289
        wd:Q59755569
      }
      ?item (wdt:P31/(wdt:P279*)) ?classes.

      OPTIONAL { ?item wdt:P4985 ?tmdb. }
      FILTER(!(BOUND(?tmdb)))

      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    """
    query += "LIMIT " + QUERY_BATCH_SIZE
    results = sparql(query)

    print("qid,P4985")
    for result in tqdm(results):
        tmdb_id = lookup_tmdb_person_id(result["imdb"])
        if not tmdb_id:
            continue
        print('{},"""{}"""'.format(result["item"], tmdb_id))


def lookup_tmdb_person_id(imdb_id, api_key=TMDB_API_KEY):
    params = {
        "api_key": api_key,
        "external_source": "imdb_id",
    }
    r = requests.get("https://api.themoviedb.org/3/find/" + imdb_id, params=params)
    r.raise_for_status()
    data = r.json()
    results = data.get("person_results")
    if not results:
        return None
    return results[0]["id"]


if __name__ == "__main__":
    main()
