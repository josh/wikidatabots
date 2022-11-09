from io import StringIO

from rdf_patch import process_graph

username = "Test"


def test_process_graph():
    rdf = """
    <http://www.wikidata.org/entity/statement/Q172241-6B571F20-7732-47E1-86B2-1DFA6D0A15F5>
    <http://wikiba.se/ontology#rank>
    <http://wikiba.se/ontology#DeprecatedRank> .
    """
    process_graph(username, StringIO(rdf), save=False)
