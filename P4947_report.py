from tqdm import tqdm

import tmdb
import wikitext
from sparql import sparql
from utils import uniq


def main():
    query = """
    SELECT ?item ?imdb ?tmdb WHERE {
      SERVICE bd:sample {
        ?item wdt:P4947 ?tmdb.
        bd:serviceParam bd:sample.limit 500 .
        bd:serviceParam bd:sample.sampleType "RANDOM".
      }
      ?item wdt:P345 ?imdb.
    }
    """
    results = sparql(query)

    query = """
    SELECT ?item ?imdb ?tmdb ?date WHERE {
      ?item wdt:P4947 ?tmdb.
      ?item wdt:P345 ?imdb.
      ?item schema:dateModified ?date.
    }
    ORDER BY DESC (?date)
    LIMIT 500
    """
    results2 = sparql(query)

    tmdb_link_rot = []
    tmdb_imdb_diff = []
    tmdb_missing_imdb_ids = []

    for result in tqdm(list(uniq(results + results2))):
        tmdb_movie = tmdb.movie(result["tmdb"])
        tmdb_movie2 = tmdb.find(id=result["imdb"], source="imdb_id", type="movie")

        if tmdb_movie and tmdb_movie.get("imdb_id") is None and result["imdb"]:
            tmdb_missing_imdb_ids.append(
                (tmdb_movie["id"], result["item"], result["imdb"])
            )

        if tmdb_movie is None:
            tmdb_link_rot.append((result["item"], result["tmdb"]))

        if tmdb_movie2 and result["tmdb"] != str(tmdb_movie2["id"]):
            tmdb_imdb_diff.append((result["item"], result["tmdb"], tmdb_movie2["id"]))

    tmdb_link_rot.sort()
    tmdb_imdb_diff.sort()
    tmdb_missing_imdb_ids.sort()

    print("== TMDb link rot ==")
    for (qid, tmdb_id) in tmdb_link_rot:
        print("* " + wikitext.item(qid) + ": " + wiki_tmdb_link(tmdb_id))
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
    for (tmdb_id, qid, imdb_id) in tmdb_missing_imdb_ids:
        imdb_url = "https://www.imdb.com/title/{}/".format(imdb_id)
        print(
            "* "
            + wiki_tmdb_link(tmdb_id, "/edit?active_nav_item=external_ids")
            + ": "
            + wikitext.item(qid)
            + " suggests "
            + wikitext.link(imdb_id, imdb_url)
        )


def wiki_tmdb_link(tmdb_id, suffix=""):
    return wikitext.link(
        tmdb_id, "https://www.themoviedb.org/movie/{}{}".format(tmdb_id, suffix)
    )


if __name__ == "__main__":
    main()
