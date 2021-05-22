from tqdm import tqdm

import sparql
import tmdb
import wikitext
from report_utils import sample_qids
from utils import uniq

P4947_URL_FORMATTER = "https://www.themoviedb.org/movie/{}"


def main():
    qids = sample_qids("P4947", count=1000)
    results = sparql.fetch_statements(qids, ["P4947", "P345"])

    tmdb_not_found = []
    tmdb_imdb_diff = []

    for qid in tqdm(results):
        item = results[qid]

        if "P4947" not in item:
            continue

        actual_ids = set()
        expected_ids = set()

        for (statement, value) in item["P4947"]:
            tmdb_movie = tmdb.object(value, type="movie")
            if tmdb_movie:
                actual_ids.add(tmdb_movie["id"])
            else:
                tmdb_not_found.append((statement, value))

        for (statement, value) in item.get("P345", []):
            tmdb_movie = tmdb.find(id=value, source="imdb_id", type="movie")
            if tmdb_movie:
                expected_ids.add(tmdb_movie["id"])

        if actual_ids and expected_ids and actual_ids != expected_ids:
            tmdb_imdb_diff.append((qid, actual_ids | expected_ids))

    tmdb_not_found.sort()
    tmdb_imdb_diff.sort()

    print("== TMDb not found ==")
    for (statement, tmdb_id) in uniq(tmdb_not_found):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(tmdb_id, P4947_URL_FORMATTER)
        )
    print("")

    print("== TMDb differences ==")
    for (qid, ids) in uniq(tmdb_imdb_diff):
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wikitext.external_ids(ids, P4947_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
