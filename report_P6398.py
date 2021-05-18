import itunes
import sparql
import wikitext
from report_utils import sample_qids
from utils import uniq


def main():
    qids = sample_qids("P6398", count=1000)
    results = sparql.fetch_statements(qids, ["P6398"])

    itunes_ids = set()
    itunes_id_statement = {}

    for qid in results:
        item = results[qid]

        if "P6398" not in item:
            continue

        for (statement, value) in item["P6398"]:
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
                "https://itunes.apple.com/us/movie/id{}".format(itunes_id),
            )
        )
    print("")


if __name__ == "__main__":
    main()
