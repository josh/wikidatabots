from wikidata import find_claim_by_guid


def test_find_claim_by_guid():
    (item, claim) = find_claim_by_guid("Q172241$bb0d6dae-4d99-0f1a-d4b5-1eea351bafbc")
    assert item and claim
    assert item.id == "Q172241"
    target = claim.getTarget()
    assert target
    assert target.text == "The Shawshank Redemption"
