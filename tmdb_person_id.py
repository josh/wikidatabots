import os
import sys

import requests
from tqdm import tqdm

from sparql import sparql

TMDB_API_KEY = os.environ.get("TMDB_API_KEY")


def missing(batch_size):
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
    query += "LIMIT " + batch_size
    results = sparql(query)

    print("qid,P4985")
    for result in tqdm(results):
        tmdb_id = lookup_tmdb_person_id(result["imdb"])
        if not tmdb_id:
            continue
        print('{},"""{}"""'.format(result["item"], tmdb_id))


def audit(batch_size):
    query = """
    SELECT ?item ?imdb ?tmdb ?random WHERE {
      ?item wdt:P345 ?imdb.
      ?item wdt:P4985 ?tmdb.
      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    """
    query += "LIMIT " + batch_size
    results = sparql(query)

    mismatches = 0

    for result in tqdm(results):
        tmdb_id = lookup_tmdb_person_id(result["imdb"])
        if result["tmdb"] != tmdb_id:
            print("-", result["item"], "P4985", result["tmdb"], file=sys.stderr)
            print("+", result["item"], "P4985", tmdb_id, file=sys.stderr)
            mismatches += 1

    query = """
    SELECT ?c WHERE {
      wd:P4985 p:P2302 ?s.
      ?s ps:P2302 wd:Q21503250.
      ?s pq:P2308 ?c.
    }
    """
    expected_classes = {
        "Q5",
        "Q16334295",
        "Q95074",
        "Q14514600",
        "Q431289",
        "Q59755569",
    }

    actual_classes = {r["c"] for r in sparql(query)}
    if actual_classes != expected_classes:
        print("instance of constraint changed", file=sys.stderr)
        mismatches += 1

    exit(mismatches)


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
    return str(results[0]["id"])


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="TMDB Person ID (P4985) Bot")
    parser.add_argument("cmd", action="store")
    parser.add_argument("--batch-size", action="store", default="100")
    args = parser.parse_args()

    if args.cmd == "missing":
        missing(batch_size=args.batch_size)
    elif args.cmd == "audit":
        audit(batch_size=args.batch_size)
    else:
        parser.print_usage()
