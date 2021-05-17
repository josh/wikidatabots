from tqdm import tqdm

import sparql
import tmdb
import wikitext
from page_extract import page_qids
from utils import uniq


def main():
    qids = (
        sparql.sample_items("P4985", type="random", limit=500)
        | sparql.sample_items("P4985", type="created", limit=500)
        | page_qids("User:Josh404Bot/Maintenance_reports/P4985")
    )

    results = sparql.fetch_statements(qids, ["P4985", "P345", "P646"])

    tmdb_link_rot = []
    tmdb_imdb_diff = []

    for qid in tqdm(results):
        item = results[qid]

        if "P4985" not in item:
            continue

        tmdb_person = None
        for (statement, value) in item["P4985"]:
            tmdb_person = tmdb.person(value)
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
        print("* " + wikitext.statement(statement) + ": " + wiki_tmdb_link(tmdb_id))
    print("")

    print("== TMDb differences ==")
    for (qid, actual_tmdb_id, expected_tmdb_id) in uniq(tmdb_imdb_diff):
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wiki_tmdb_link(actual_tmdb_id)
            + " vs "
            + wiki_tmdb_link(expected_tmdb_id)
        )
    print("")


def wiki_tmdb_link(tmdb_id, suffix=""):
    return wikitext.link(
        tmdb_id, "https://www.themoviedb.org/person/{}{}".format(tmdb_id, suffix)
    )


if __name__ == "__main__":
    main()
