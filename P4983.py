from tqdm import tqdm

import tmdb
from sparql import sparql


def main():
    """
    Find Wikidata items that are missing a TMDb TV series ID (P4983) but have a
    IMDb ID (P345) or TheTVDB.com series ID (P4835). Attempt to look up the
    TV show via the TMDb API. If there's a match, create a new statement.

    Outputs QuickStatements CSV commands.
    """

    items = {}

    def accumulate_results(results):
        for result in results:
            qid = result["item"]
            if qid not in items:
                items[qid] = {"imdb": set(), "tvdb": set()}
            item = items[qid]

            if result["imdb"]:
                item["imdb"].add(result["imdb"])

            if result["tvdb"]:
                item["tvdb"].add(result["tvdb"])

    query = """
    SELECT ?item ?imdb ?tvdb ?random WHERE {
      ?item wdt:P345 ?imdb.
      OPTIONAL { ?item wdt:P4835 ?tvdb. }

      VALUES ?classes {
        wd:Q15416
      }
      ?item (wdt:P31/(wdt:P279*)) ?classes.

      OPTIONAL { ?item wdt:P4985 ?tmdb. }
      FILTER(!(BOUND(?tmdb)))

      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    LIMIT 500
    """
    accumulate_results(sparql(query))

    query = """
    SELECT ?item ?imdb ?tvdb ?random WHERE {
      ?item wdt:P4835 ?tvdb.
      OPTIONAL { ?item wdt:P345 ?imdb. }

      VALUES ?classes {
        wd:Q15416
      }
      ?item (wdt:P31/(wdt:P279*)) ?classes.

      OPTIONAL { ?item wdt:P4985 ?tmdb. }
      FILTER(!(BOUND(?tmdb)))

      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    LIMIT 500
    """
    accumulate_results(sparql(query))

    print("qid,P4983")
    for qid in tqdm(items):
        item = items[qid]
        tmdb_ids = set()

        for imdb_id in item["imdb"]:
            person = tmdb.find(id=imdb_id, source="imdb_id", type="tv")
            if person:
                tmdb_ids.add(person["id"])

        for tvdb_id in item["tvdb"]:
            person = tmdb.find(id=tvdb_id, source="tvdb_id", type="tv")
            if person:
                tmdb_ids.add(person["id"])

        for tmdb_id in tmdb_ids:
            print('{},"""{}"""'.format(qid, tmdb_id))


if __name__ == "__main__":
    main()
