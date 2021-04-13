import re

import requests
from tqdm import tqdm

from sparql import sparql


def audit(batch_size):
    query = """
    SELECT ?item ?imdb WHERE {
      SERVICE bd:sample {
        ?item wdt:P345 ?imdb.
        bd:serviceParam bd:sample.limit ?limit .
        bd:serviceParam bd:sample.sampleType "RANDOM".
      }
    }
    """
    query = query.replace("?limit", str(batch_size))
    results = sparql(query)

    for result in tqdm(results):
        id = result["imdb"]
        id2 = canonical_id(id)
        if id is not id2:
            print("-", result["item"], "P345", id)
            print("+", result["item"], "P345", id2)


def canonical_id(id):
    url = formatted_url(id)
    if not url:
        return id

    r = requests.head(url)
    if r.status_code == 200:
        return id
    elif r.status_code == 301:
        return extract_id(r.headers["Location"]) or id
    elif r.status_code == 404:
        return None
    else:
        return id


def formatted_url(id):
    m = re.match("^(tt\\d+)$", id)
    if m:
        return "https://www.imdb.com/title/{}/".format(m.group(1))

    m = re.match("^(nm\\d+)$", id)
    if m:
        return "https://www.imdb.com/name/{}/".format(m.group(1))

    return None


def extract_id(url):
    m = re.match("^/title/(tt\\d+)/$", url)
    if m:
        return m.group(1)

    m = re.match("^/name/(nm\\d+)/$", url)
    if m:
        return m.group(1)

    return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="IMDb ID (P345) Bot")
    parser.add_argument("cmd", action="store")
    parser.add_argument("--batch-size", action="store", default="100")
    args = parser.parse_args()

    if args.cmd == "audit":
        audit(batch_size=args.batch_size)
    else:
        parser.print_usage()
