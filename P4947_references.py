from tqdm import tqdm

import tmdb
from quickstatements import today
from sparql import sparql


def main():
    """
    Find Wikidata items have a TMDb movie ID (P4947) without references.
    Attempt to verify the ID by looking it up by the IMDb ID via the TMDb API.
    If there's a match, create a reference.

    Outputs QuickStatements CSV commands.
    """

    query = """
    SELECT DISTINCT ?item ?statement ?imdb ?tmdb WHERE {
      ?item wdt:P345 ?imdb.
      ?item p:P4947 ?statement.
      ?statement ps:P4947 ?tmdb.
      OPTIONAL {
        ?statement prov:wasDerivedFrom ?reference.
        ?reference pr:P248 wd:Q20828898;
                   pr:P345 [].
      }
      FILTER(!BOUND(?reference))
    }
    LIMIT 100
    """
    results = sparql(query)

    print("qid,P4947,S248,s345,s813")
    for result in tqdm(results):
        movie = tmdb.find(id=result["imdb"], source="imdb_id", type="movie")
        if movie and movie["id"] == int(result["tmdb"]):
            print(
                '{},"""{}""",{},"""{}""",{}'.format(
                    result["item"],
                    movie["id"],
                    "Q20828898",
                    result["imdb"],
                    today(),
                )
            )


if __name__ == "__main__":
    main()
