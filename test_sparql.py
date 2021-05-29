from sparql import fetch_statements, sample_items, sparql, get_claims


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


def test_sample_items():
    results = sample_items("P345", limit=5, type="random")
    assert len(results) == 5

    results = sample_items("P345", limit=5, type="created")
    assert len(results) == 5

    results = sample_items("P345", limit=5, type="updated")
    assert len(results) == 5


def test_fetch_statements():
    items = fetch_statements(["Q172241"], ["P345", "P4947"])
    assert len(items) == 1

    item = items["Q172241"]
    assert item
    assert item["P345"]
    assert item["P4947"]


def test_get_claims():
    statements = get_claims(["Q172241"], ["P345", "P4947"])
    assert len(statements) == 2
    assert statements["Q172241$6B571F20-7732-47E1-86B2-1DFA6D0A15F5"]
    assert statements["q172241$D5D28036-0EB8-42D4-A757-6EE65377FBEC"]

    statements = statements.filter_item("Q172241")
    assert len(statements) == 2
    assert statements["Q172241$6B571F20-7732-47E1-86B2-1DFA6D0A15F5"]
    assert statements["q172241$D5D28036-0EB8-42D4-A757-6EE65377FBEC"]

    for (property, pstatements) in statements.by_property():
        assert len(pstatements) == 1

    statements = statements.filter_property("P345")
    assert len(statements) == 1
    assert statements["q172241$D5D28036-0EB8-42D4-A757-6EE65377FBEC"]
