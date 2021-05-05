from tqdm import tqdm

import tmdb
from sparql import sparql
from utils import uniq


def main():
    query = """
    SELECT ?item ?imdb ?tmdb WHERE {
      SERVICE bd:sample {
        ?item wdt:P4985 ?tmdb.
        bd:serviceParam bd:sample.limit 1000 .
        bd:serviceParam bd:sample.sampleType "RANDOM".
      }
      ?item wdt:P345 ?imdb.
    }
    """
    results = sparql(query)

    query = """
    SELECT ?item ?imdb ?tmdb ?date WHERE {
      ?item wdt:P4985 ?tmdb.
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
        tmdb_person = tmdb.person(result["tmdb"])
        expected_tmdb_id = tmdb.find_by_imdb_id(result["imdb"], type="person")

        if tmdb_person and tmdb_person.get("imdb_id") is None and result["imdb"]:
            tmdb_missing_imdb_ids.append(
                (tmdb_person["id"], result["item"], result["imdb"])
            )

        if tmdb_person is None:
            tmdb_link_rot.append((result["item"], result["tmdb"]))

        if expected_tmdb_id and result["tmdb"] != expected_tmdb_id:
            tmdb_imdb_diff.append((result["item"], result["tmdb"], expected_tmdb_id))

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
        imdb_url = "https://www.imdb.com/name/{}/".format(imdb_id)
        print(
            "* "
            + wiki_tmdb_link(tmdb_id, "/edit?active_nav_item=external_ids")
            + ": "
            + wiki_qid(qid)
            + " suggests "
            + wiki_link(imdb_id, imdb_url)
        )


def wiki_link(title, url):
    return "[{url} {title}]".format(url=url, title=title)


def wiki_qid(qid):
    return "{{Q|" + qid.replace("Q", "") + "}}"


def wiki_tmdb_link(tmdb_id, suffix=""):
    return wiki_link(
        tmdb_id, "https://www.themoviedb.org/person/{}{}".format(tmdb_id, suffix)
    )


if __name__ == "__main__":
    main()
