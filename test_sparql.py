# pyright: strict

import polars as pl
from rdflib import URIRef

import wikidata
from constants import IMDB_ID_PID, TMDB_MOVIE_ID_PID
from sparql import (
    extract_qid,
    fetch_statements,
    sample_items,
    sparql,
    sparql_df,
    type_constraints,
)


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
    uri = results[0]["statement"].toPython()
    assert uri == (
        "http://www.wikidata.org/entity/statement/"
        "Q1-789eef0c-4108-cdda-1a63-505cdd324564"
    )


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
    qid = wikidata.qid("Q172241")
    items = fetch_statements([qid], [IMDB_ID_PID, TMDB_MOVIE_ID_PID])
    assert len(items) == 1

    item = items[qid]
    assert item
    assert item[IMDB_ID_PID]
    assert item[TMDB_MOVIE_ID_PID]


def test_sparql_some_value():
    results = sparql(
        """
        SELECT ?gender WHERE {
          wd:Q100330360 wdt:P21 ?gender.
          FILTER(wikibase:isSomeValue(?gender))
        }
        LIMIT 1
        """
    )
    assert len(results) == 1
    result = results[0]
    assert result
    assert result["gender"] == URIRef(
        "http://www.wikidata.org/.well-known/genid/804129ad66d7a442efd976927d7a6fb0"
    )


def test_sparql_df():
    lf = sparql_df(
        """
        SELECT ?item ?itemLabel WHERE {
          ?item wdt:P31 wd:Q146.
          SERVICE wikibase:label {
            bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
          }
        }
        LIMIT 10
        """,
        columns=["item", "itemLabel"],
    ).with_columns(extract_qid("item").alias("qid"))
    assert lf.schema == {"item": pl.Utf8, "itemLabel": pl.Utf8, "qid": pl.Utf8}
    df = lf.collect()
    assert len(df) == 10
