# pyright: reportUnknownArgumentType=false

from io import StringIO

import rdf_patch
from rdf_patch import process_graph

username = "Test"


def setup_function(function) -> None:
    rdf_patch.get_item_page.cache_clear()
    rdf_patch.get_property_page.cache_clear()
    rdf_patch.resolve_claim_guid.cache_clear()


def test_change_statement_rank() -> None:
    triple = [
        "wds:Q172241-6B571F20-7732-47E1-86B2-1DFA6D0A15F5",
        "wikibase:rank",
        "wikibase:DeprecatedRank;",
        "wikidatabots:editSummary",
        '"Changed rank"',
        ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q172241"
    assert summary == "Changed rank"
    assert len(claims) == 1
    assert claims[0]["rank"] == "deprecated"


def test_noop_change_statement_rank() -> None:
    triple = [
        "wds:Q172241-6B571F20-7732-47E1-86B2-1DFA6D0A15F5",
        "wikibase:rank",
        "wikibase:NormalRank",
        ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 0


def test_add_prop_direct_value() -> None:
    triple = [
        "wd:Q172241",
        "wdt:P4947",
        '"123";',
        "wikidatabots:editSummary",
        '"Add TMDb movie ID"' ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q172241"
    assert summary == "Add TMDb movie ID"
    assert len(claims) == 1
    assert claims[0]["mainsnak"]["property"] == "P4947"
    assert claims[0]["mainsnak"]["datavalue"]["value"] == "123"


def test_noop_change_prop_direct_value() -> None:
    triple = ["wd:Q172241", "wdt:P4947", '"278"', "."]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 0


# TODO: This should probably add a new statement
def test_noop_change_prop_direct_deprecated_value() -> None:
    triple = ["wd:Q1292541", "wdt:P4947", '"429486"', "."]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 0


def test_add_prop_statement_value() -> None:
    triples = [
        "wd:Q172241",
        "p:P4947",
        "_:a",
        ".",
        "_:a",
        "ps:P4947",
        '"123"',
        ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triples))))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q172241"
    assert summary is None
    assert len(claims) == 1
    assert claims[0]["mainsnak"]["property"] == "P4947"
    assert claims[0]["mainsnak"]["datavalue"]["value"] == "123"


def test_add_prop_qualifer() -> None:
    triple = [
        "wds:q172241-E0C7392E-5020-4DC1-8520-EEBF57C3AB66",
        "pq:P4633",
        '"Narrator"',
        ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q172241"
    assert summary is None
    assert len(claims) == 1
    assert claims[0]["mainsnak"]["property"] == "P161"
    assert (
        claims[0]["qualifiers"]["P4633"][0]["datavalue"]["value"]
        == 'Ellis Boyd "Red" Redding'
    )
    assert claims[0]["qualifiers"]["P4633"][1]["datavalue"]["value"] == "Narrator"


def test_noop_change_prop_qualifer() -> None:
    triple = [
        "wds:q172241-91B6C9F4-2F78-4577-9726-6E9D8D76B486",
        "pq:P4633",
        '"Andy Dufresne"',
        ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 0


def test_delete_prop_qualifer() -> None:
    triple = """
    wds:Q1292541-2203A57C-488F-4371-9F88-9A5EB91C4883 pqe:P2241 [] .
    """
    edits = list(process_graph(username, StringIO(triple)))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q1292541"
    assert summary is None
    assert len(claims) == 1
    assert claims[0]["mainsnak"]["property"] == "P4947"
    assert claims[0].get("qualifiers") is None


def test_noop_change_prop_statement() -> None:
    triple = [
        "wds:q172241-E0C7392E-5020-4DC1-8520-EEBF57C3AB66",
        "ps:P161",
        "wd:Q48337",
        ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 0


def test_add_item_prop_qualifer() -> None:
    triples = [
        "wd:Q172241",
        "p:P161",
        "_:a",
        ".",
        "_:a",
        "ps:P161",
        "wd:Q48337",
        ".",
        "_:a",
        "pq:P4633",
        '"Narrator"',
        ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triples))))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q172241"
    assert summary is None
    assert len(claims) == 1
    assert claims[0]["mainsnak"]["property"] == "P161"
    assert claims[0]["mainsnak"]["datavalue"]["value"]["numeric-id"] == 48337
    assert claims[0]["qualifiers"]["P4633"][0]["datavalue"]["value"] == "Narrator"


def test_update_item_prop_qualifer_exclusive() -> None:
    triples = """
      wd:Q172241 p:P161 [ ps:P161 wd:Q48337 ; pqe:P4633 "Narrator" ] .
    """
    edits = list(process_graph(username, StringIO(triples)))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q172241"
    assert summary is None
    assert len(claims) == 1
    assert claims[0]["mainsnak"]["property"] == "P161"
    assert claims[0]["mainsnak"]["datavalue"]["value"]["numeric-id"] == 48337
    assert claims[0]["qualifiers"]["P4633"][0]["datavalue"]["value"] == "Narrator"


def test_quantity_value() -> None:
    triples = """
      wikidatabots:testSubject wikidatabots:assertValue _:b1.
      _:b1 rdf:type wikibase:QuantityValue;
        wikibase:quantityAmount "+123"^^xsd:decimal;
        wikibase:quantityUpperBound "+124"^^xsd:decimal;
        wikibase:quantityLowerBound "+122"^^xsd:decimal;
        wikibase:quantityUnit wd:Q828224.

      wikidatabots:testSubject wikidatabots:assertValue _:b2.
      _:b2 rdf:type wikibase:QuantityValue;
        wikibase:quantityAmount "+123"^^xsd:decimal;
        wikibase:quantityUnit wd:Q828224.

      wikidatabots:testSubject wikidatabots:assertValue _:b3.
      _:b3 rdf:type wikibase:QuantityValue;
        wikibase:quantityAmount "+123"^^xsd:decimal.
    """
    _ = list(process_graph(username, StringIO(triples)))


def test_time_value() -> None:
    triples = """
      wikidatabots:testSubject wikidatabots:assertValue _:b1.
      _:b1 rdf:type wikibase:TimeValue;
        wikibase:timeValue "2020-01-01T00:00:00Z"^^xsd:dateTime;
        wikibase:timePrecision "11"^^xsd:integer;
        wikibase:timeTimezone "0"^^xsd:integer;
        wikibase:timeCalendarModel wd:Q1985727.

      wikidatabots:testSubject wikidatabots:assertValue _:b2.
      _:b2 rdf:type wikibase:TimeValue;
        wikibase:timeValue "2020-01-01";
        wikibase:timePrecision "11"^^xsd:integer;
        wikibase:timeTimezone "0"^^xsd:integer;
        wikibase:timeCalendarModel wd:Q1985727.

      wikidatabots:testSubject wikidatabots:assertValue
        "2020-01-01T00:00:00Z"^^xsd:dateTime.
      wikidatabots:testSubject wikidatabots:assertValue "2020-01-01"^^xsd:date.
    """
    _ = list(process_graph(username, StringIO(triples)))


def test_reference_value() -> None:
    triples = """
      wikidatabots:testSubject wikidatabots:assertValue _:b1.
      _:b1 a wikibase:Reference ;
        pr:P248 wd:Q37312 ;
        pr:P345 "tt0111161" ;
        pr:P813 "2021-06-23T00:00:00Z"^^xsd:dateTime ;
        prv:P813 _:b2 .

      _:b2 a wikibase:TimeValue ;
        wikibase:timeValue "2021-06-23T00:00:00Z"^^xsd:dateTime ;
        wikibase:timePrecision "11"^^xsd:integer ;
        wikibase:timeTimezone "0"^^xsd:integer ;
        wikibase:timeCalendarModel <http://www.wikidata.org/entity/Q1985727> .
    """
    _ = list(process_graph(username, StringIO(triples)))


def test_resolve_items() -> None:
    triples = """
      wikidatabots:testSubject wikidatabots:assertValue wd:Q42.
      wikidatabots:testSubject wikidatabots:assertValue wd:P31.
    """
    _ = list(process_graph(username, StringIO(triples)))
