from tqdm import tqdm

import imdb
from sparql import sparql


def main():
    query = """
    SELECT ?item ?imdb WHERE {
      SERVICE bd:sample {
        ?item wdt:P345 ?imdb.
        bd:serviceParam bd:sample.limit 500 .
        bd:serviceParam bd:sample.sampleType "RANDOM".
      }
    }
    """
    results = sparql(query)

    imdb_link_rot = []
    imdb_redirects = []
    imdb_link_unknown = []

    for result in tqdm(results):
        id = result["imdb"]

        if imdb.is_valid_id(id):
            new_id = imdb.canonical_id(id)

            if new_id is None:
                imdb_link_rot.append((result["item"], id))
            elif id is not new_id:
                imdb_redirects.append((result["item"], id, new_id))

        else:
            imdb_link_unknown.append((result["item"], id))

    print("== IMDb link rot ==")
    for (qid, imdb_id) in imdb_link_rot:
        print(
            "* "
            + wiki_qid(qid)
            + ": "
            + wiki_link(imdb_id, imdb.formatted_url(imdb_id))
        )
    print("")

    print("== IMDb redirects ==")
    for (qid, imdb_id, imdb_canonical_id) in imdb_redirects:
        print(
            "* "
            + wiki_qid(qid)
            + ": "
            + wiki_link(imdb_id, imdb.formatted_url(imdb_id))
            + " → "
            + wiki_link(imdb_id, imdb.formatted_url(imdb_canonical_id))
        )
    print("")

    print("== IMDb unknown IDs ==")
    for (qid, imdb_id) in imdb_link_unknown:
        print("* " + wiki_qid(qid) + ": " + imdb_id)
    print("")


def wiki_link(title, url):
    return "[{url} {title}]".format(url=url, title=title)


def wiki_qid(qid):
    return "{{Q|" + qid.replace("Q", "") + "}}"


if __name__ == "__main__":
    main()
