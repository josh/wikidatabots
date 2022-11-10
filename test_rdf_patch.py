from io import StringIO

import rdf_patch
from rdf_patch import process_graph

username = "Test"


def setup_function(function):
    rdf_patch.get_item_page.cache_clear()
    rdf_patch.get_property_page.cache_clear()
    rdf_patch.resolve_entity.cache_clear()
    rdf_patch.resolve_entity_statement.cache_clear()


def test_change_statement_rank():
    triple = [
        "wds:Q172241-6B571F20-7732-47E1-86B2-1DFA6D0A15F5",
        "wikibase:rank",
        "wikibase:DeprecatedRank",
        ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q172241"
    assert summary is None
    assert len(claims) == 1
    assert claims[0]["rank"] == "deprecated"


def test_noop_change_statement_rank():
    triple = [
        "wds:Q172241-6B571F20-7732-47E1-86B2-1DFA6D0A15F5",
        "wikibase:rank",
        "wikibase:NormalRank",
        ".",
    ]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 0


def test_add_prop_direct_value():
    triple = ["wd:Q172241", "wdt:P4947", '"123"', "."]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q172241"
    assert summary is None
    assert len(claims) == 1
    assert claims[0]["mainsnak"]["property"] == "P4947"
    assert claims[0]["mainsnak"]["datavalue"]["value"] == "123"


def test_noop_change_prop_direct_value():
    triple = ["wd:Q172241", "wdt:P4947", '"278"', "."]
    edits = list(process_graph(username, StringIO(" ".join(triple))))
    assert len(edits) == 0
