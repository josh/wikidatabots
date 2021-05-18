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

    tmdb_link_rot = []
    tmdb_imdb_diff = []

    for qid in tqdm(results):
        item = results[qid]

        if "P4985" not in item:
            continue

        tmdb_person = None
        for (statement, value) in item["P4985"]:
            tmdb_person = tmdb.object(value, type="person")
            if not tmdb_person:
                tmdb_link_rot.append((statement, value))

        tmdb_person_via_imdb = None
        for (statement, value) in item.get("P345", []):
            tmdb_person_via_imdb = tmdb.find(id=value, source="imdb_id", type="person")

        tmdb_person_via_freebase = None
        for (statement, value) in item.get("P646", []):
            tmdb_person_via_freebase = tmdb.find(
                id=value, source="freebase_mid", type="person"
            )

        if (
            tmdb_person
            and tmdb_person_via_imdb
            and tmdb_person["id"] != tmdb_person_via_imdb["id"]
        ):
            tmdb_imdb_diff.append((qid, tmdb_person["id"], tmdb_person_via_imdb["id"]))

        if (
            tmdb_person
            and tmdb_person_via_freebase
            and tmdb_person["id"] != tmdb_person_via_freebase["id"]
        ):
            tmdb_imdb_diff.append(
                (qid, tmdb_person["id"], tmdb_person_via_freebase["id"])
            )

    tmdb_link_rot.sort()
    tmdb_imdb_diff.sort()

    print("== TMDb link rot ==")
    for (statement, tmdb_id) in uniq(tmdb_link_rot):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(tmdb_id, P4985_URL_FORMATTER)
        )
    print("")

    print("== TMDb differences ==")
    for (qid, actual_tmdb_id, expected_tmdb_id) in uniq(tmdb_imdb_diff):
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wikitext.external_id(actual_tmdb_id, P4985_URL_FORMATTER)
            + " vs "
            + wikitext.external_id(expected_tmdb_id, P4985_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
