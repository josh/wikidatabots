from sparql import sparql

INSTANCE_OF_QUERY = """
SELECT ?c WHERE {
  wd:P4983 p:P2302 ?s.
  ?s ps:P2302 wd:Q21503250.
  ?s pq:P2308 ?c.
}
"""


def test_instance_of_constraints():
    expected_classes = {"Q15416"}
    actual_classes = {r["c"] for r in sparql(INSTANCE_OF_QUERY)}
    assert actual_classes == expected_classes
