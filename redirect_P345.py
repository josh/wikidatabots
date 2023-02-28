# pyright: strict

import logging

import imdb
import sparql
import wikidata
from constants import IMDB_ID_PID, REASON_FOR_DEPRECATED_RANK_PID, REDIRECT_QID


def imdb_qids_missing_tmdb() -> set[wikidata.QID]:
    query = """
    SELECT DISTINCT ?item WHERE {
        ?item wdt:P345 ?imdb.

        OPTIONAL { ?item wdt:P4947 ?tmdb_movie_id. }
        OPTIONAL { ?item wdt:P4983 ?tmdb_tv_id. }
        OPTIONAL { ?item wdt:P4985 ?tmdb_person_id. }

        FILTER(!(BOUND(?tmdb_movie_id)))
        FILTER(!(BOUND(?tmdb_tv_id)))
        FILTER(!(BOUND(?tmdb_person_id)))

        BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    LIMIT 100
    """
    results = sparql.sparql(query)
    return set([result["item"] for result in results])


def main():
    # qids = sparql.sample_items(IMDB_ID_PID, limit=100)
    qids = imdb_qids_missing_tmdb()
    results = sparql.fetch_statements(qids, [IMDB_ID_PID])

    edit_summary = "Add claim for canonical IMDb ID"

    for qid in results:
        item = results[qid]

        for statement, value in item.get(IMDB_ID_PID, []):
            assert isinstance(value, str)

            if not imdb.formatted_url(value):
                logging.debug(f"{value} is invalid format")
                continue
            id = value

            new_id = imdb.canonical_id(id)
            if not new_id:
                logging.debug(f"{id} not found")
                continue

            if id is not new_id:
                print(
                    f'wd:{qid} wdt:P345 "{new_id}" ; '
                    f'wikidatabots:editSummary "{edit_summary}" . '
                )
                print(
                    f"{statement.n3()} "
                    f"wikibase:rank wikibase:DeprecatedRank ; "
                    f"pq:{REASON_FOR_DEPRECATED_RANK_PID} "
                    f"wd:{REDIRECT_QID} ; "
                    f'wikidatabots:editSummary "{edit_summary}" . '
                )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
