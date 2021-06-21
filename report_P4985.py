from tqdm import tqdm

import sparql
import tmdb
import wikitext
from report_utils import sample_qids
from utils import uniq

P4985_URL_FORMATTER = "https://www.themoviedb.org/person/{}"


def main():
    qids = sample_qids("P4985", count=1000)
    results = sparql.fetch_statements(qids, ["P4985", "P345", "P646"])

    tmdb_not_found = []

    for qid in tqdm(results):
        item = results[qid]

        if "P4985" not in item:
            continue

        for (statement, value) in item["P4985"]:
            tmdb_person = tmdb.object(value, type="person")
            if tmdb_person:
                pass
            else:
                tmdb_not_found.append((statement, value))

    tmdb_not_found.sort()

    print("== TMDb not found ==")
    for (statement, tmdb_id) in uniq(tmdb_not_found):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(tmdb_id, P4985_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
