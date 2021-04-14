import sys

from tqdm import tqdm

from sparql import sparql
from tmdb import find_by_imdb_id


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
    #ORDER BY ?random
    """
    query += "LIMIT " + batch_size
    results = sparql(query)

    print("qid,P4985")
    for result in tqdm(results):
        tmdb_id = find_by_imdb_id(result["imdb"], type="person")
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
        tmdb_id = find_by_imdb_id(result["imdb"], type="person")
        if result["tmdb"] != tmdb_id:
            print("-", result["item"], "P4985", result["tmdb"], file=sys.stderr)
            print("+", result["item"], "P4985", tmdb_id, file=sys.stderr)
            mismatches += 1

    exit(mismatches)


if __name__ == "__main__":
    import argparse

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
