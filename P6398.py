from tqdm import tqdm

import appletv
import sparql
from report_utils import page_qids


def main():
    """
    Find Wikidata items that are missing a iTunes movie ID (P6398) but have a
    Apple TV movie ID (P9586).

    Outputs QuickStatements CSV commands.
    """

    page_title = "User:Josh404Bot/Preliminarily matched/P6398"
    qids = page_qids(page_title)

    query = """
    SELECT ?item ?random WHERE {
      ?item wdt:P9586 ?appletv.

      VALUES ?classes {
        wd:Q11424
        wd:Q1261214
      }
      ?item (wdt:P31/(wdt:P279*)) ?classes.

      OPTIONAL { ?item wdt:P6398 ?itunes. }
      FILTER(!(BOUND(?itunes)))

      BIND(MD5(CONCAT(STR(?item), STR(RAND()))) AS ?random)
    }
    ORDER BY (?random)
    LIMIT 500
    """
    for result in sparql.sparql(query):
        qids.add(result["item"])

    results = sparql.fetch_statements(qids, ["P6398", "P9586"])

    print("qid,P6398")
    for qid in tqdm(results):
        item = results[qid]

        if item.get("P6398"):
            continue

        for (statement, value) in item.get("P9586", []):
            movie = appletv.movie(value)
            if movie and movie["itunes_id"]:
                print('{},"""{}"""'.format(qid, movie["itunes_id"]))


if __name__ == "__main__":
    main()
