import logging

from tqdm import tqdm

import imdb
import sparql
from constants import IMDB_ID_PID, REASON_FOR_DEPRECATED_RANK_PID, REDIRECT_QID


def main():
    qids = sparql.sample_items(IMDB_ID_PID, limit=100)
    results = sparql.fetch_statements(qids, [IMDB_ID_PID])

    print("PREFIX wd: <http://www.wikidata.org/entity/>")
    print("PREFIX wds: <http://www.wikidata.org/entity/statement/>")
    print("PREFIX p: <http://www.wikidata.org/prop/>")
    print("PREFIX pq: <http://www.wikidata.org/prop/qualifier/>")
    print("PREFIX ps: <http://www.wikidata.org/prop/statement/>")
    print("PREFIX wikibase: <http://wikiba.se/ontology#>")
    print("PREFIX wikidatabots: <https://github.com/josh/wikidatabots#>")

    edit_summary = "Add claim for canonical IMDb ID"

    for qid in tqdm(results):
        item = results[qid]

        for (statement, value) in item.get(IMDB_ID_PID, []):
            id = imdb.tryid(value)
            if not id:
                logging.debug(f"{value} is invalid format")
                continue

            new_id = imdb.canonical_id(id)
            if not new_id:
                logging.debug(f"{id} not found")
                continue

            # TODO: Get original statement IRIs
            assert "$" in statement
            guid = statement.replace("$", "-")

            if id is not new_id:
                print(
                    f"wd:Q2851520 p:P345 "
                    f'[ ps:P345 "{new_id}" ] ; '
                    f'wikidatabots:editSummary "{edit_summary}" . '
                )
                print(
                    f"wds:{guid} "
                    f"wikibase:rank wikibase:DeprecatedRank ; "
                    f"pq:{REASON_FOR_DEPRECATED_RANK_PID} "
                    f"wd:{REDIRECT_QID} ; "
                    f'wikidatabots:editSummary "{edit_summary}" . '
                )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
