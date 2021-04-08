import os

import requests

QUERY_BATCH_SIZE = os.environ.get("QUERY_BATCH_SIZE", "100")
TMDB_API_KEY = os.environ["TMDB_API_KEY"]
WIKIDATA_USER_AGENT = os.environ["WIKIDATA_USER_AGENT"]
QUICKSTATEMENTS_USERNAME = os.environ["QUICKSTATEMENTS_USERNAME"]
QUICKSTATEMENTS_TOKEN = os.environ["QUICKSTATEMENTS_TOKEN"]


def missing_tmdb_id():
    query = """
    SELECT ?item ?imdb (MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random) WHERE {
      VALUES ?classes { wd:Q11424 wd:Q1261214 } .
      ?item wdt:P31 ?classes .
      ?item wdt:P345 ?imdb.
      FILTER NOT EXISTS { ?item p:P4947 []. }
    }
    ORDER BY (?random)
    """
    query += " LIMIT " + QUERY_BATCH_SIZE
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": WIKIDATA_USER_AGENT,
    }
    r = requests.get(
        "https://query.wikidata.org/sparql", headers=headers, params={"query": query}
    )
    r.raise_for_status()
    data = r.json()
    return data["results"]["bindings"]


def lookup_tmdb_id(imdb_id):
    params = {
        "api_key": TMDB_API_KEY,
        "external_source": "imdb_id",
    }
    r = requests.get("https://api.themoviedb.org/3/find/" + imdb_id, params=params)
    r.raise_for_status()
    data = r.json()
    results = data.get("movie_results", [])
    if not results:
        return None
    return results[0]["id"]


def import_batch(statements):
    data = {
        "action": "import",
        "submit": "1",
        "username": QUICKSTATEMENTS_USERNAME,
        "token": QUICKSTATEMENTS_TOKEN,
        "batchname": "TMDbIDs",
        "format": "csv",
        "data": "qid,P4947\n",
    }

    for (entity, value) in statements:
        data["data"] += '{},"""{}"""\n'.format(entity, value)

    url = "https://quickstatements.toolforge.org/api.php"

    if os.environ.get("DRY_RUN") == "1":
        print("batchname", data["batchname"])
        print(data["data"])
        return True

    r = requests.post(url, data=data)
    r.raise_for_status()

    resp = r.json()
    assert resp["status"] == "OK"


def main():
    statements = []

    for entity in missing_tmdb_id():
        qid = entity["item"]["value"].replace("http://www.wikidata.org/entity/", "")
        tmdb_id = lookup_tmdb_id(entity["imdb"]["value"])
        if tmdb_id:
            statements.append((qid, tmdb_id))

    import_batch(statements)


if __name__ == "__main__":
    main()
