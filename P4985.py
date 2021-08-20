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

      VALUES ?classes {
        wd:Q5
        wd:Q16334295
        wd:Q95074
        wd:Q14514600
        wd:Q431289
        wd:Q59755569
      }
      # ?item (wdt:P31/(wdt:P279*)) ?classes.
      ?item wdt:P31 ?classes.

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
        if result["item"] in blocked_qids():
            logging.debug("{} is blocked".format(result["item"]))
            continue

        person = tmdb.find(id=result["imdb"], source="imdb_id", type="person")
        if not person:
            continue
        print('{},"""{}"""'.format(result["item"], person["id"]))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
