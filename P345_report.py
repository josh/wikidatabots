from tqdm import tqdm

import imdb
from sparql import sparql


def main():
    query = """
    SELECT ?statement ?value WHERE {
      SERVICE bd:sample {
        ?item p:P345 ?statement.
        bd:serviceParam bd:sample.limit 500 ;
          bd:sample.sampleType "RANDOM".
      }
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
      ?statement ps:P345 ?value.
    }
    """
    results = sparql(query)

    imdb_link_rot = []
    imdb_redirects = []
    imdb_link_unknown = []

    for result in tqdm(results):
        id = result["value"]

        if imdb.is_valid_id(id):
            new_id = imdb.canonical_id(id)

            if new_id is None:
                imdb_link_rot.append((result["statement"], id))
            elif id is not new_id:
                imdb_redirects.append((result["statement"], id, new_id))

        else:
            imdb_link_unknown.append((result["statement"], id))

    imdb_link_rot.sort()
    imdb_redirects.sort()
    imdb_link_unknown.sort()

    print("== IMDb link rot ==")
    for (statement, imdb_id) in imdb_link_rot:
        print(
            "* "
            + wiki_statement(statement)
            + ": "
            + wiki_link(imdb_id, imdb.formatted_url(imdb_id))
        )
    print("")

    print("== IMDb redirects ==")
    for (statement, imdb_id, imdb_canonical_id) in imdb_redirects:
        print(
            "* "
            + wiki_statement(statement)
            + ": "
            + wiki_link(imdb_id, imdb.formatted_url(imdb_id))
            + " → "
            + wiki_link(imdb_canonical_id, imdb.formatted_url(imdb_canonical_id))
        )
    print("")

    print("== IMDb unknown IDs ==")
    for (statement, imdb_id) in imdb_link_unknown:
        print("* " + wiki_statement(statement) + ": " + imdb_id)
    print("")


def wiki_link(title, url):
    return "[{url} {title}]".format(url=url, title=title)


def wiki_statement(statement):
    statement = statement.replace("$", "-")
    return wiki_link(
        "wds:{}".format(statement),
        "http://www.wikidata.org/entity/statement/{}".format(statement),
    )


if __name__ == "__main__":
    main()
