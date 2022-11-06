from constants import IMDB_ID_PID, TMDB_MOVIE_ID_PID
from sparql import fetch_statements, sample_items, sparql, type_constraints


def test_sparql():
    results = sparql(
        """
        SELECT ?item ?itemLabel WHERE {
          ?item wdt:P31 wd:Q146.
          SERVICE wikibase:label {
            bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
          }
        }
        LIMIT 10
        """
    )
    assert len(results) == 10
    assert results[0]["item"]
    assert type(results[0]["item"]) is str
    assert results[0]["item"].startswith("Q")
    assert results[0]["itemLabel"]


def test_sparql_property():
    results = sparql(
        """
        SELECT ?statement WHERE {
          wd:Q1 p:P580 ?statement.
        }
        """
    )
    assert len(results) == 1
    assert results[0]["statement"] == "Q1$789eef0c-4108-cdda-1a63-505cdd324564"


def test_type_constraints():
    classes = type_constraints(TMDB_MOVIE_ID_PID)
    assert "Q11424" in classes
    assert "Q2431196" not in classes
    assert "Q202866" in classes
    assert "Q1261214" in classes


def test_sample_items():
    results = sample_items(IMDB_ID_PID, limit=5, type="random")
    assert len(results) == 5

    results = sample_items(IMDB_ID_PID, limit=5, type="created")
    assert len(results) == 5

    results = sample_items(IMDB_ID_PID, limit=5, type="updated")
    assert len(results) == 5


def test_fetch_statements():
    items = fetch_statements(["Q172241"], [IMDB_ID_PID, TMDB_MOVIE_ID_PID])
    assert len(items) == 1

    item = items["Q172241"]
    assert item
    assert item[IMDB_ID_PID]
    assert item[TMDB_MOVIE_ID_PID]
