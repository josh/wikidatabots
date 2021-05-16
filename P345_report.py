from tqdm import tqdm

import imdb
import wikitext
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
            + wikitext.statement(statement)
            + ": "
            + wikitext.link(imdb_id, imdb.formatted_url(imdb_id))
        )
    print("")

    print("== IMDb redirects ==")
    for (statement, imdb_id, imdb_canonical_id) in imdb_redirects:
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.link(imdb_id, imdb.formatted_url(imdb_id))
            + " → "
            + wikitext.link(imdb_canonical_id, imdb.formatted_url(imdb_canonical_id))
        )
    print("")

    print("== IMDb unknown IDs ==")
    for (statement, imdb_id) in imdb_link_unknown:
        print("* " + wikitext.statement(statement) + ": " + imdb_id)
    print("")


if __name__ == "__main__":
    main()
