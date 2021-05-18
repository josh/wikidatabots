from tqdm import tqdm

import sparql
import tmdb
import wikitext
from report_utils import sample_qids
from utils import uniq

P4983_URL_FORMATTER = "https://www.themoviedb.org/tv/{}"


def main():
    qids = sample_qids("P4983", count=1000)
    results = sparql.fetch_statements(qids, ["P4983", "P345", "P646"])

    tmdb_link_rot = []
    tmdb_imdb_diff = []

    for qid in tqdm(results):
        item = results[qid]

        if "P4983" not in item:
            continue

        tmdb_show = None
        for (statement, value) in item["P4983"]:
            tmdb_show = tmdb.object(value, type="tv")
            if not tmdb_show:
                tmdb_link_rot.append((statement, value))

        tmdb_show_via_imdb = None
        for (statement, value) in item.get("P345", []):
            tmdb_show_via_imdb = tmdb.find(id=value, source="imdb_id", type="tv")

        tmdb_show_via_freebase = None
        for (statement, value) in item.get("P646", []):
            tmdb_show_via_freebase = tmdb.find(
                id=value, source="freebase_mid", type="tv"
            )

        if (
            tmdb_show
            and tmdb_show_via_imdb
            and tmdb_show["id"] != tmdb_show_via_imdb["id"]
        ):
            tmdb_imdb_diff.append((qid, tmdb_show["id"], tmdb_show_via_imdb["id"]))

        if (
            tmdb_show
            and tmdb_show_via_freebase
            and tmdb_show["id"] != tmdb_show_via_freebase["id"]
        ):
            tmdb_imdb_diff.append((qid, tmdb_show["id"], tmdb_show_via_freebase["id"]))

    tmdb_link_rot.sort()
    tmdb_imdb_diff.sort()

    print("== TMDb link rot ==")
    for (statement, tmdb_id) in uniq(tmdb_link_rot):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(tmdb_id, P4983_URL_FORMATTER)
        )
    print("")

    print("== TMDb differences ==")
    for (qid, actual_tmdb_id, expected_tmdb_id) in uniq(tmdb_imdb_diff):
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wikitext.external_id(actual_tmdb_id, P4983_URL_FORMATTER)
            + " vs "
            + wikitext.external_id(expected_tmdb_id, P4983_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
