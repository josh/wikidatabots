import sparql
import tmdb
from report_utils import sample_qids


def main():
    qids = sample_qids("P4947", count=500)
    results = sparql.fetch_statements(qids, ["P4947"])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get("P4947", []):
            try:
                id = int(value)
            except ValueError:
                continue

            if not tmdb.object(id, type="movie"):
                print("{},Q21441764".format(statement))


if __name__ == "__main__":
    main()
