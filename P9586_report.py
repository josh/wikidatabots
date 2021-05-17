from tqdm import tqdm

import appletv
import sparql
import wikitext


def main():
    qids = sparql.sample_items("P9586", limit=100) | sparql.recent_items(
        "P9586", limit=100
    )

    results = sparql.fetch_statements(qids, ["P6398", "P9586"])

    link_rot = []
    itunes_statements = []
    itunes_diff = []

    for qid in tqdm(results):
        item = results[qid]

        appletv_movie = None
        for (statement, value) in item["P9586"]:
            appletv_movie = appletv.movie(value)
            if not appletv_movie:
                link_rot.append((statement, value))

        itunes_ids = set()
        for (statement, value) in item.get("P6398", []):
            itunes_ids.add(int(value))

        if not appletv_movie:
            continue

        if len(itunes_ids) == 0 and appletv_movie["itunes_id"]:
            itunes_statements.append((qid, appletv_movie["itunes_id"]))
        elif (
            len(itunes_ids) > 0
            and appletv_movie["itunes_id"]
            and appletv_movie["itunes_id"] not in itunes_ids
        ):
            itunes_diff.append((qid, list(itunes_ids)[0], appletv_movie["itunes_id"]))

    link_rot.sort()
    itunes_statements.sort()
    itunes_diff.sort()

    print("== Link rot ==")
    for (statement, id) in link_rot:
        print("* " + wikitext.statement(statement) + ": " + wiki_appletv_link(id))
    print("")

    print("== iTunes Store statements ==")
    for (qid, value) in itunes_statements:
        print("* {{Statement|" + qid + '|P6398|"' + str(value) + '"}}')
    print("")

    print("== iTunes Store differences ==")
    for (qid, actual_itunes_id, expected_itunes_id) in itunes_diff:
        print(
            "* "
            + wikitext.item(qid)
            + ": "
            + wiki_itunes_link(actual_itunes_id)
            + " vs "
            + wiki_itunes_link(expected_itunes_id)
        )
    print("")


def wiki_appletv_link(appletv_id):
    return wikitext.link(
        appletv_id, "https://tv.apple.com/us/movie/{}".format(appletv_id)
    )


def wiki_itunes_link(itunes_id):
    return wikitext.link(
        itunes_id, "http://itunes.apple.com/us/movie/id{}".format(itunes_id)
    )


if __name__ == "__main__":
    main()
