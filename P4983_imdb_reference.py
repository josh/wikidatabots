from tqdm import tqdm

import tmdb
from quickstatements import today
from sparql import sparql


def main():
    """
    Find Wikidata items have a TMDb TV series ID (P4983) without references.
    Attempt to verify the ID by looking it up by the IMDb ID via the TMDb API.
    If there's a match, create a reference.

    Outputs QuickStatements CSV commands.
    """

    query = """
    SELECT DISTINCT ?item ?statement ?imdb ?tmdb WHERE {
      ?item wdt:P345 ?imdb.
      ?item p:P4983 ?statement.
      ?statement ps:P4983 ?tmdb.
      OPTIONAL {
        ?statement prov:wasDerivedFrom ?reference.
        ?reference pr:P248 wd:Q20828898;
                   pr:P345 [].
      }
      FILTER(!BOUND(?reference))
    }
    LIMIT 2500
    """
    results = sparql(query)

    print("qid,P4983,S248,s345,s813")
    for result in tqdm(results):
        tv = tmdb.find(id=result["imdb"], source="imdb_id", type="tv")
        if tv and tv["id"] == int(result["tmdb"]):
            print(
                '{},"""{}""",{},"""{}""",{}'.format(
                    result["item"],
                    tv["id"],
                    "Q20828898",
                    result["imdb"],
                    today(),
                )
            )


if __name__ == "__main__":
    main()
