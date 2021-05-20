from tqdm import tqdm

import sparql
import tmdb
import wikitext
from report_utils import sample_qids
from utils import uniq

P4983_URL_FORMATTER = "https://www.themoviedb.org/tv/{}"


def main():
    qids = sample_qids("P4983", count=1000)
    results = sparql.fetch_statements(qids, ["P4983", "P345", "P646", "P4835"])

    tmdb_link_rot = []
    tmdb_imdb_diff = []

    for qid in tqdm(results):
        item = results[qid]

        if "P4983" not in item:
            continue

        actual_ids = set()
        expected_ids = set()

        for (statement, value) in item["P4983"]:
            tmdb_show = tmdb.object(value, type="tv")
            if tmdb_show:
                actual_ids.add(tmdb_show["id"])
            else:
                tmdb_link_rot.append((statement, value))

        for (statement, value) in item.get("P345", []):
            tmdb_show = tmdb.find(id=value, source="imdb_id", type="tv")
            if tmdb_show:
                expected_ids.add(tmdb_show["id"])

        for (statement, value) in item.get("P646", []):
            tmdb_show = tmdb.find(id=value, source="freebase_mid", type="tv")
            if tmdb_show:
                expected_ids.add(tmdb_show["id"])

        for (statement, value) in item.get("P4835", []):
            tmdb_show = tmdb.find(id=value, source="tvdb_id", type="tv")
            if tmdb_show:
                expected_ids.add(tmdb_show["id"])

        if actual_ids and expected_ids and actual_ids != expected_ids:
            tmdb_imdb_diff.append((qid, actual_ids | expected_ids))

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
    for (qid, ids) in uniq(tmdb_imdb_diff):
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wikitext.external_ids(ids, P4983_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
