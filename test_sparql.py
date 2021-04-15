from sparql import sparql


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
