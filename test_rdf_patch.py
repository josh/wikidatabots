from io import StringIO

import rdf_patch
from rdf_patch import process_graph

username = "Test"


def setup_function(function):
    rdf_patch.get_item_page.cache_clear()
    rdf_patch.get_property_page.cache_clear()
    rdf_patch.resolve_entity.cache_clear()
    rdf_patch.resolve_entity_statement.cache_clear()


def test_deprecate_statement():
    rdf = """
    wds:Q172241-6B571F20-7732-47E1-86B2-1DFA6D0A15F5
    wikibase:rank
    wikibase:DeprecatedRank .
    """
    edits = list(process_graph(username, StringIO(rdf)))
    assert len(edits) == 1
    (item, claims, summary) = edits[0]
    assert item.id == "Q172241"
    assert summary is None
    assert len(claims) == 1
    assert claims[0]["rank"] == "deprecated"


def test_noop_change_statement_rank():
    rdf = """
    wds:Q172241-6B571F20-7732-47E1-86B2-1DFA6D0A15F5
    wikibase:rank
    wikibase:NormalRank .
    """
    edits = list(process_graph(username, StringIO(rdf)))
    assert len(edits) == 0
