import itunes
import sparql
import wikitext
from page_extract import page_qids
from utils import uniq


def main():
    qids = (
        sparql.sample_items("P6998", type="random", limit=1000)
        | sparql.sample_items("P6998", type="created", limit=1000)
        | page_qids("User:Josh404Bot/Maintenance_reports/P6998")
    )

    results = sparql.fetch_statements(qids, ["P6998"])

    itunes_ids = set()
    itunes_id_statement = {}

    for qid in results:
        item = results[qid]

        if "P6998" not in item:
            continue

        for (statement, value) in item["P6998"]:
            id = int(value)
            itunes_ids.add(id)
            itunes_id_statement[id] = statement

    itunes_link_rot = []

    for (id, obj) in itunes.batch_lookup(itunes_ids):
        if not obj:
            itunes_link_rot.append((itunes_id_statement[id], id))

    itunes_link_rot.sort()

    print("== iTunes link rot ==")
    for (statement, itunes_id) in uniq(itunes_link_rot):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.link(
                itunes_id,
                "https://itunes.apple.com/us/movie-collection/id{}".format(itunes_id),
            )
        )
    print("")


if __name__ == "__main__":
    main()
