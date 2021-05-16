from tqdm import tqdm

import sparql
import tmdb
import wikitext


def main():
    qids = sparql.sample_items("P4947", limit=500) | sparql.recent_items(
        "P4947", limit=500
    )

    results = sparql.fetch_statements(qids, ["P4947", "P345", "P646"])

    tmdb_link_rot = []
    tmdb_imdb_diff = []
    tmdb_missing_imdb_ids = []

    for qid in tqdm(results):
        item = results[qid]

        tmdb_movie = None
        for (statement, value) in item["P4947"]:
            tmdb_movie = tmdb.movie(value)
            if not tmdb_movie:
                tmdb_link_rot.append((statement, value))

        tmdb_movie_via_imdb = None
        for (statement, value) in item.get("P345", []):
            tmdb_movie_via_imdb = tmdb.find(id=value, source="imdb_id", type="movie")
            if tmdb_movie and not tmdb_movie_via_imdb:
                tmdb_missing_imdb_ids.append((tmdb_movie["id"], statement, value))

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
    tmdb_missing_imdb_ids.sort()

    print("== TMDb link rot ==")
    for (statement, tmdb_id) in tmdb_link_rot:
        print("* " + wikitext.statement(statement) + ": " + wiki_tmdb_link(tmdb_id))
    print("")

    print("== TMDb differences ==")
    for (qid, actual_tmdb_id, expected_tmdb_id) in tmdb_imdb_diff:
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wiki_tmdb_link(actual_tmdb_id)
            + " vs "
            + wiki_tmdb_link(expected_tmdb_id)
        )
    print("")

    print("== TMDb missing IMDB IDs ==")
    for (tmdb_id, statement, imdb_id) in tmdb_missing_imdb_ids:
        imdb_url = "https://www.imdb.com/title/{}/".format(imdb_id)
        print(
            "* "
            + wiki_tmdb_link(tmdb_id, "/edit?active_nav_item=external_ids")
            + ": "
            + wikitext.statement(statement)
            + " suggests "
            + wikitext.link(imdb_id, imdb_url)
        )


def wiki_tmdb_link(tmdb_id, suffix=""):
    return wikitext.link(
        tmdb_id, "https://www.themoviedb.org/movie/{}{}".format(tmdb_id, suffix)
    )


if __name__ == "__main__":
    main()
