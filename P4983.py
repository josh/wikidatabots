import logging
from typing import TypedDict

from tqdm import tqdm

import tmdb
from page import blocked_qids
from sparql import sparql


class Item(TypedDict):
    imdb: set[str]
    tvdb: set[str]


def main():
    """
    Find Wikidata items that are missing a TMDb TV series ID (P4983) but have a
    IMDb ID (P345) or TheTVDB.com series ID (P4835). Attempt to look up the
    TV show via the TMDb API. If there's a match, create a new statement.

    Outputs QuickStatements CSV commands.
    """

    query = """
    SELECT ?item ?imdb ?tvdb ?random WHERE {
      # Items with either IMDb or TVDB IDs
      { ?item wdt:P4835 []. }
      UNION
      { ?item wdt:P345 []. }

      VALUES ?classes {
        wd:Q15416
      }
      ?item (wdt:P31/(wdt:P279*)) ?classes.

      # Get IMDb and TVDB IDs
      OPTIONAL { ?item wdt:P345 ?imdb. }
      OPTIONAL { ?item wdt:P4835 ?tvdb. }

      # Exclude items that already have a TMDB TV ID
      OPTIONAL { ?item wdt:P4983 ?tmdb. }
      FILTER(!(BOUND(?tmdb)))

      # Generate random sorting key
      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    LIMIT 5000
    """

    items: dict[str, Item] = {}

    for result in sparql(query):
        qid = result["item"]
        assert type(qid) is str

        if qid in blocked_qids():
            logging.debug(f"{qid} is blocked")
            continue

        if qid not in items:
            items[qid] = {"imdb": set(), "tvdb": set()}
        item = items[qid]

        if result["imdb"]:
            assert type(result["imdb"]) is str
            item["imdb"].add(result["imdb"])

        if result["tvdb"]:
            assert type(result["tvdb"]) is str
            item["tvdb"].add(result["tvdb"])

    print("qid,P4983")
    for qid in tqdm(items):
        item = items[qid]
        tmdb_ids: set[int] = set()

        for imdb_id in item["imdb"]:
            tv = tmdb.find(id=imdb_id, source="imdb_id", type="tv")
            if tv:
                tmdb_ids.add(tv["id"])

        for tvdb_id in item["tvdb"]:
            tv = tmdb.find(id=tvdb_id, source="tvdb_id", type="tv")
            if tv:
                tmdb_ids.add(tv["id"])

        for tmdb_id in tmdb_ids:
            print(f'{qid},"""{tmdb_id}"""')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
