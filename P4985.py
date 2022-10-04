import logging

from tqdm import tqdm

import tmdb
from page import blocked_qids
from sparql import sparql


def main():
    """
    Find Wikidata items that are missing a TMDb person ID (P4985) but have a
    IMDb ID (P345). Attempt to look up the person by IMDb ID via the TMDb API.
    If there's a match, create a new statement.

    Outputs QuickStatements CSV commands.
    """

    query = """
    SELECT DISTINCT ?item ?imdb ?random WHERE {
      ?item wdt:P345 ?imdb.

      # ?item (wdt:P31/(wdt:P279*)) wd:Q5.
      ?item wdt:P31 wd:Q5.

      OPTIONAL { ?item wdt:P4985 ?tmdb. }
      FILTER(!(BOUND(?tmdb)))

      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    LIMIT 5000
    """
    results = sparql(query)

    print("qid,P4985")
    for result in tqdm(results):
        qid: str = result["item"]

        if qid in blocked_qids():
            logging.debug(f"{qid} is blocked")
            continue

        assert result["imdb"]

        person = tmdb.find(id=result["imdb"], source="imdb_id", type="person")
        if not person:
            continue
        print(f'{result["item"]},"""{person["id"]}"""')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
