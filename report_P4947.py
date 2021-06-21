from tqdm import tqdm

import sparql
import tmdb
import wikitext
from report_utils import sample_qids
from utils import uniq

P4947_URL_FORMATTER = "https://www.themoviedb.org/movie/{}"


def main():
    qids = sample_qids("P4947", count=2500)
    results = sparql.fetch_statements(qids, ["P4947", "P345"])

    tmdb_not_found = []

    for qid in tqdm(results):
        item = results[qid]

        if "P4947" not in item:
            continue

        for (statement, value) in item["P4947"]:
            tmdb_movie = tmdb.object(value, type="movie")
            if tmdb_movie:
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
            + wikitext.external_id(tmdb_id, P4947_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
