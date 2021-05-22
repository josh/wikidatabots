import itunes
import sparql
import wikitext
from report_utils import sample_qids
from utils import uniq

P6398_URL_FORMATTER = "https://itunes.apple.com/us/movie/id{}"


def main():
    qids = sample_qids("P6398", count=5000)
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

    itunes_not_found = []

    for (id, obj) in itunes.batch_lookup(itunes_ids):
        if not obj:
            itunes_not_found.append((itunes_id_statement[id], id))

    itunes_not_found.sort()

    print("== iTunes not found ==")
    for (statement, itunes_id) in uniq(itunes_not_found):
        print(
            "* "
            + wikitext.statement(statement)
            + ": "
            + wikitext.external_id(itunes_id, P6398_URL_FORMATTER)
        )
    print("")


if __name__ == "__main__":
    main()
