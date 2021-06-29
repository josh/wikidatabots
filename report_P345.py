from tqdm import tqdm

import imdb
import sparql
import wikitext
from report_utils import sample_qids
from utils import uniq


def main():
    qids = sample_qids("P345", constraint_violations=False, count=500)
    results = sparql.fetch_statements(qids, ["P345"])

    imdb_not_found = []
    imdb_redirects = []

    for qid in tqdm(results):
        item = results[qid]

        if "P345" not in item:
            continue

        for (statement, id) in item["P345"]:
            if imdb.is_valid_id(id):
                new_id = imdb.canonical_id(id)

                if new_id is None:
                    imdb_not_found.append((statement, id))
                elif id is not new_id:
                    imdb_redirects.append((statement, id, new_id))

    imdb_not_found.sort()
    imdb_redirects.sort()

    print("== IMDb not found ==")
    for (statement, imdb_id) in uniq(imdb_not_found):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.link(imdb_id, imdb.formatted_url(imdb_id))
        )
    print("")

    print("== IMDb redirects ==")
    for (statement, imdb_id, imdb_canonical_id) in uniq(imdb_redirects):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.link(imdb_id, imdb.formatted_url(imdb_id))
            + " → "
            + wikitext.link(imdb_canonical_id, imdb.formatted_url(imdb_canonical_id))
        )
    print("")


if __name__ == "__main__":
    main()
