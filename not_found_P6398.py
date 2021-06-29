import itunes
import sparql
from report_utils import sample_qids


def main():
    qids = sample_qids("P6398", count=2500)
    results = sparql.fetch_statements(qids, ["P6398"])

    itunes_ids = {}

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get("P6398", []):
            try:
                id = int(value)
                itunes_ids[id] = statement
            except ValueError:
                continue

    for (id, obj) in itunes.batch_lookup(itunes_ids.keys()):
        if not obj:
            print("{},Q21441764".format(itunes_ids[id]))


if __name__ == "__main__":
    main()
