import sparql
import tmdb
from page import page_qids
from sparql import sample_qids


def main():
    assert tmdb.object(31, type="person")

    qids = sample_qids("P4985", count=2500)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P4985")

    results = sparql.fetch_statements(qids, ["P4985"])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get("P4985", []):
            try:
                id = int(value)
            except ValueError:
                continue

            if not tmdb.object(id, type="person"):
                print("{},Q21441764".format(statement))


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
