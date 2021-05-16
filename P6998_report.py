import itunes
from sparql import sparql


def main():
    query = """
    SELECT ?statement ?value WHERE {
      SERVICE bd:sample {
        ?item p:P6998 ?statement.
        bd:serviceParam bd:sample.limit 5000 ;
          bd:sample.sampleType "RANDOM".
      }
      ?statement wikibase:rank ?rank.
      FILTER(?rank != wikibase:DeprecatedRank)
      ?statement ps:P6998 ?value.
    }
    """
    results = sparql(query)

    itunes_ids = set()
    itunes_id_statement = {}

    for result in results:
        id = int(result["value"])
        itunes_ids.add(id)
        itunes_id_statement[id] = result["statement"]

    itunes_link_rot = []

    for (id, obj) in itunes.batch_lookup(itunes_ids):
        if obj:
            pass
        else:
            itunes_link_rot.append((itunes_id_statement[id], id))

    itunes_link_rot.sort()

    print("== iTunes link rot ==")
    for (statement, itunes_id) in itunes_link_rot:
        print(
            "* "
            + wiki_statement(statement)
            + ": "
            + wiki_link(
                itunes_id, "https://itunes.apple.com/us/movie/id{}".format(itunes_id)
            )
        )
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
