import logging

from pywikibot import ItemPage
from tqdm import tqdm

import tmdb
from page import blocked_qids
from properties import TMDB_MOVIE_ID_PROPERTY
from quickstatements import print_item_external_id_statements
from sparql import sparql
from wikidata import SITE


def main():
    """
    Find Wikidata items that are missing a TMDb movie ID (P4947) but have a
    IMDb ID (P345). Attempt to look up the movie by IMDb ID via the TMDb API.
    If there's a match, create a new statement.

    Outputs QuickStatements CSV commands.
    """

    query = """
    SELECT DISTINCT ?item ?imdb ?random WHERE {
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
    ORDER BY ?random
    LIMIT 5000
    """
    results = sparql(query)

    def statements():
        for result in tqdm(results):
            item: ItemPage = ItemPage(SITE, result["item"])
            if item.id in blocked_qids():
                logging.debug(f"{item.id} is blocked")
                continue

            assert type(result["imdb"]) is str

            movie = tmdb.find(id=result["imdb"], source="imdb_id", type="movie")
            if not movie:
                continue
            yield item, movie["id"]

    print_item_external_id_statements(TMDB_MOVIE_ID_PROPERTY, statements())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
