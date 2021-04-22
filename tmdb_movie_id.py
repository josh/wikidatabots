from tqdm import tqdm

import tmdb
from sparql import sparql


def missing():
    query = """
    SELECT DISTINCT ?item ?imdb ?random WHERE {
      ?item wdt:P345 ?imdb.

      VALUES ?classes {
        wd:Q11424
        wd:Q1261214
      }
      ?item (wdt:P31/(wdt:P279*)) ?classes.

      OPTIONAL { ?item wdt:P4947 ?tmdb. }
      FILTER(!(BOUND(?tmdb)))

      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY ?random
    LIMIT 500
    """
    results = sparql(query)

    print("qid,P4947")
    for result in tqdm(results):
        tmdb_id = tmdb.find_by_imdb_id(result["imdb"], type="movie")
        if not tmdb_id:
            continue
        print('{},"""{}"""'.format(result["item"], tmdb_id))


def report():
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

    tmdb_link_rot = []
    tmdb_imdb_diff = []
    tmdb_missing_imdb_ids = []

    for result in tqdm(results):
        tmdb_movie = tmdb.movie(result["tmdb"])
        expected_tmdb_id = tmdb.find_by_imdb_id(result["imdb"], type="movie")

        if tmdb_movie and tmdb_movie.get("imdb_id") is None and result["imdb"]:
            tmdb_missing_imdb_ids.append(
                (tmdb_movie["id"], result["item"], result["imdb"])
            )

        if tmdb_movie is None:
            tmdb_link_rot.append((result["item"], result["tmdb"]))

        if expected_tmdb_id and result["tmdb"] != expected_tmdb_id:
            tmdb_imdb_diff.append((result["item"], result["tmdb"], expected_tmdb_id))

    def wiki_link(title, url):
        return "[{url} {title}]".format(url=url, title=title)

    def wiki_qid(qid):
        return "{{Q|" + qid.replace("Q", "") + "}}"

    def wiki_tmdb_link(tmdb_id, suffix=""):
        return wiki_link(
            tmdb_id, "https://www.themoviedb.org/movie/{}{}".format(tmdb_id, suffix)
        )

    print("== TMDb link rot ==")
    for (qid, tmdb_id) in tmdb_link_rot:
        print("* " + wiki_qid(qid) + ": " + wiki_tmdb_link(tmdb_id))
    print("")

    print("== TMDb differences ==")
    for (qid, actual_tmdb_id, expected_tmdb_id) in tmdb_imdb_diff:
        print(
            "* "
            + wiki_qid(qid)
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
            + wiki_qid(qid)
            + " suggests "
            + wiki_link(imdb_id, imdb_url)
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TMDB Movie ID (P4947) Bot")
    parser.add_argument("cmd", action="store")
    args = parser.parse_args()

    if args.cmd == "missing":
        missing()
    elif args.cmd == "report":
        report()
    else:
        parser.print_usage()
