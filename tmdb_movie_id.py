import sys

from tqdm import tqdm

from sparql import sparql
from tmdb import find_by_imdb_id


def missing(batch_size):
    query = """
    SELECT ?item ?imdb ?random WHERE {
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
    #ORDER BY ?random
    """
    query += "LIMIT " + batch_size
    results = sparql(query)

    print("qid,P4947")
    for result in tqdm(results):
        tmdb_id = find_by_imdb_id(result["imdb"], type="movie")
        if not tmdb_id:
            continue
        print('{},"""{}"""'.format(result["item"], tmdb_id))


def audit(batch_size):
    query = """
    SELECT ?item ?imdb ?tmdb ?random WHERE {
      ?item wdt:P345 ?imdb.
      ?item wdt:P4947 ?tmdb.
      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    """
    query += "LIMIT " + batch_size
    results = sparql(query)

    mismatches = 0

    for result in tqdm(results):
        tmdb_id = find_by_imdb_id(result["imdb"], type="movie")

        if tmdb_id is None:
            print(
                "No TMDb movie for {}, maybe add to {}".format(
                    result["imdb"], result["tmdb"]
                ),
                file=sys.stderr,
            )
        elif result["tmdb"] != tmdb_id:
            print("-", result["item"], "P4947", result["tmdb"], file=sys.stderr)
            print("+", result["item"], "P4947", tmdb_id, file=sys.stderr)
            mismatches += 1

    query = """
    SELECT ?c WHERE {
      wd:P4947 p:P2302 ?s.
      ?s ps:P2302 wd:Q21503250.
      ?s pq:P2308 ?c.
    }
    """
    expected_classes = {"Q11424", "Q1261214"}

    actual_classes = {r["c"] for r in sparql(query)}
    if actual_classes != expected_classes:
        print("instance of constraint changed", file=sys.stderr)
        mismatches += 1

    exit(mismatches)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TMDB Movie ID (P4947) Bot")
    parser.add_argument("cmd", action="store")
    parser.add_argument("--batch-size", action="store", default="100")
    args = parser.parse_args()

    if args.cmd == "missing":
        missing(batch_size=args.batch_size)
    elif args.cmd == "audit":
        audit(batch_size=args.batch_size)
    else:
        parser.print_usage()
