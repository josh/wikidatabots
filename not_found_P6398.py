# pyright: strict

import itunes
import sparql
import wikidata
from constants import (
    ITUNES_MOVIE_ID_PID,
    REASON_FOR_DEPRECATED_RANK_PID,
    WITHDRAWN_IDENTIFIER_VALUE_QID,
)
from page import page_qids
from sparql import sample_items
from timeout import iter_until_deadline
from utils import tryint


def main():
    (id, obj) = next(itunes.batch_lookup([567661493]))
    assert id and obj

    qids = sample_items(ITUNES_MOVIE_ID_PID, limit=10000)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P6398")

    statements = sparql.fetch_statements(qids, [ITUNES_MOVIE_ID_PID])
    itunes_ids = extract_itunes_ids(statements)

    print("PREFIX wd: <http://www.wikidata.org/entity/>")
    print("PREFIX wds: <http://www.wikidata.org/entity/statement/>")
    print("PREFIX wikibase: <http://wikiba.se/ontology#>")
    print("PREFIX pq: <http://www.wikidata.org/prop/qualifier/>")
    print("PREFIX wikidatabots: <https://github.com/josh/wikidatabots#>")

    edit_summary = "Deprecate iTunes movie ID delisted from store"

    for (id, obj) in iter_until_deadline(itunes.batch_lookup(itunes_ids.keys())):
        if not obj and itunes.all_not_found(id):
            # TODO: Get original statement IRIs
            guid = itunes_ids[id]
            assert "$" in guid
            guid = guid.replace("$", "-")
            print(
                f"wds:{guid} "
                f"wikibase:rank wikibase:DeprecatedRank ; "
                f"pq:{REASON_FOR_DEPRECATED_RANK_PID} ; "
                f"wd:{WITHDRAWN_IDENTIFIER_VALUE_QID} ; "
                f'wikidatabots:editSummary "{edit_summary}" . '
            )


def extract_itunes_ids(
    statements: dict[
        wikidata.QID, dict[wikidata.PID, list[tuple[wikidata.StatementGUID, str]]]
    ]
) -> dict[int, str]:
    itunes_ids: dict[int, str] = {}
    for item in statements.values():
        for (statement, value) in item.get(ITUNES_MOVIE_ID_PID, []):
            id = tryint(value)
            if id:
                itunes_ids[id] = statement
    return itunes_ids


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
