from tqdm import tqdm

import sparql
import tmdb
import wikitext
from report_utils import sample_qids
from utils import uniq

P4985_URL_FORMATTER = "https://www.themoviedb.org/person/{}"


def main():
    qids = sample_qids("P4985", count=2500)
    results = sparql.fetch_statements(qids, ["P4985", "P345", "P646"])

    tmdb_not_found = []
    tmdb_imdb_diff = []

    for qid in tqdm(results):
        item = results[qid]

        if "P4985" not in item:
            continue

        actual_ids = set()
        expected_ids = set()

        for (statement, value) in item["P4985"]:
            tmdb_person = tmdb.object(value, type="person")
            if tmdb_person:
                actual_ids.add(tmdb_person["id"])
            else:
                tmdb_not_found.append((statement, value))

        for (statement, value) in item.get("P345", []):
            tmdb_person = tmdb.find(id=value, source="imdb_id", type="person")
            if tmdb_person:
                expected_ids.add(tmdb_person["id"])

        for (statement, value) in item.get("P646", []):
            tmdb_person = tmdb.find(id=value, source="freebase_mid", type="person")
            if tmdb_person:
                expected_ids.add(tmdb_person["id"])

        if actual_ids and expected_ids and not expected_ids.issubset(actual_ids):
            tmdb_imdb_diff.append((qid, actual_ids | expected_ids))

    tmdb_not_found.sort()
    tmdb_imdb_diff.sort()

    print("== TMDb not found ==")
    for (statement, tmdb_id) in uniq(tmdb_not_found):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(tmdb_id, P4985_URL_FORMATTER)
        )
    print("")

    print("== TMDb differences ==")
    for (qid, ids) in uniq(tmdb_imdb_diff):
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wikitext.external_ids(ids, P4985_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
