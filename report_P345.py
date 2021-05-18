from tqdm import tqdm

import imdb
import sparql
import wikitext
from report_utils import sample_qids
from utils import uniq


def main():
    qids = sample_qids("P345", count=500)
    results = sparql.fetch_statements(qids, ["P345"])

    imdb_link_rot = []
    imdb_redirects = []
    imdb_link_unknown = []

    for qid in tqdm(results):
        item = results[qid]

        if "P345" not in item:
            continue

        for (statement, id) in item["P345"]:
            if imdb.is_valid_id(id):
                new_id = imdb.canonical_id(id)

                if new_id is None:
                    imdb_link_rot.append((statement, id))
                elif id is not new_id:
                    imdb_redirects.append((statement, id, new_id))

            else:
                imdb_link_unknown.append((statement, id))

    imdb_link_rot.sort()
    imdb_redirects.sort()
    imdb_link_unknown.sort()

    print("== IMDb link rot ==")
    for (statement, imdb_id) in uniq(imdb_link_rot):
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

    print("== IMDb unknown IDs ==")
    for (statement, imdb_id) in uniq(imdb_link_unknown):
        print("* " + wikitext.statement(statement) + ": " + imdb_id)
    print("")


if __name__ == "__main__":
    main()
