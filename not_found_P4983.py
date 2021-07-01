import sparql
import tmdb
from page import page_qids
from sparql import sample_items


def main():
    assert tmdb.object(1399, type="tv")

    qids = sample_items("P4983", limit=1000)
    qids |= page_qids("Wikidata:Database reports/Constraint violations/P4983")

    results = sparql.fetch_statements(qids, ["P4983"])

    for qid in results:
        item = results[qid]

        for (statement, value) in item.get("P4983", []):
            try:
                id = int(value)
            except ValueError:
                continue

            if not tmdb.object(id, type="tv"):
                print("{},Q21441764".format(statement))


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    main()
