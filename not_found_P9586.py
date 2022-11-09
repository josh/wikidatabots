# pyright: strict

import appletv
import sparql
from constants import (
    APPLE_TV_MOVIE_ID_PID,
    REASON_FOR_DEPRECATED_RANK_PID,
    WITHDRAWN_IDENTIFIER_VALUE_QID,
)
from page import page_qids
from sparql import sample_items
from timeout import iter_until_deadline


def main():
    assert not appletv.all_not_found(
        type="movie", id=appletv.id("umc.cmc.o5z5ztufuu3uv8lx7m0jcega")
    )

    qids = sample_items(APPLE_TV_MOVIE_ID_PID, limit=250)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P9586")

    results = sparql.fetch_statements(qids, [APPLE_TV_MOVIE_ID_PID])

    print("PREFIX wd: <http://www.wikidata.org/entity/>")
    print("PREFIX wds: <http://www.wikidata.org/entity/statement/>")
    print("PREFIX wikibase: <http://wikiba.se/ontology#>")
    print("PREFIX pq: <http://www.wikidata.org/prop/qualifier/>")
    print("PREFIX wikidatabots: <https://github.com/josh/wikidatabots#>")

    edit_summary = "Deprecate Apple TV movie ID delisted from store"

    for qid in iter_until_deadline(results):
        item = results[qid]

        for (statement, value) in item.get(APPLE_TV_MOVIE_ID_PID, []):
            id = appletv.tryid(value)
            if not id:
                continue

            if appletv.all_not_found(type="movie", id=id):
                print(
                    f"{statement.n3()} "
                    f"wikibase:rank wikibase:DeprecatedRank ; "
                    f"pq:{REASON_FOR_DEPRECATED_RANK_PID} "
                    f"wd:{WITHDRAWN_IDENTIFIER_VALUE_QID} ; "
                    f'wikidatabots:editSummary "{edit_summary}" . '
                )


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
