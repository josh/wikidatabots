from tqdm import tqdm

import sparql
import tmdb
import wikitext
from report_utils import sample_qids
from utils import uniq

P4947_URL_FORMATTER = "https://www.themoviedb.org/movie/{}"


def main():
    qids = sample_qids("P4947", count=1000)
    results = sparql.fetch_statements(qids, ["P4947", "P345", "P646"])

    tmdb_link_rot = []
    tmdb_imdb_diff = []

    for qid in tqdm(results):
        item = results[qid]

        if "P4947" not in item:
            continue

        tmdb_movie = None
        for (statement, value) in item["P4947"]:
            tmdb_movie = tmdb.movie(value)
            if not tmdb_movie:
                tmdb_link_rot.append((statement, value))

        tmdb_movie_via_imdb = None
        for (statement, value) in item.get("P345", []):
            tmdb_movie_via_imdb = tmdb.find(id=value, source="imdb_id", type="movie")

        tmdb_movie_via_freebase = None
        for (statement, value) in item.get("P646", []):
            tmdb_movie_via_freebase = tmdb.find(
                id=value, source="freebase_mid", type="movie"
            )

        if (
            tmdb_movie
            and tmdb_movie_via_imdb
            and tmdb_movie["id"] != tmdb_movie_via_imdb["id"]
        ):
            tmdb_imdb_diff.append((qid, tmdb_movie["id"], tmdb_movie_via_imdb["id"]))

        if (
            tmdb_movie
            and tmdb_movie_via_freebase
            and tmdb_movie["id"] != tmdb_movie_via_freebase["id"]
        ):
            tmdb_imdb_diff.append(
                (qid, tmdb_movie["id"], tmdb_movie_via_freebase["id"])
            )

    tmdb_link_rot.sort()
    tmdb_imdb_diff.sort()

    print("== TMDb link rot ==")
    for (statement, tmdb_id) in uniq(tmdb_link_rot):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(tmdb_id, P4947_URL_FORMATTER)
        )
    print("")

    print("== TMDb differences ==")
    for (qid, actual_tmdb_id, expected_tmdb_id) in uniq(tmdb_imdb_diff):
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wikitext.external_id(actual_tmdb_id, P4947_URL_FORMATTER)
            + " vs "
            + wikitext.external_id(expected_tmdb_id, P4947_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
