# pyright: strict

from rdflib.term import URIRef

from wikidata import NS_MANAGER, qid


def test_qid():
    assert qid("Q42")


def test_namespace_manager():
    uri = URIRef("http://wikiba.se/ontology#Item")
    assert uri.n3(NS_MANAGER) == "wikibase:Item"
    assert NS_MANAGER.expand_curie("wikibase:Item") == uri

    uri = URIRef("http://www.wikidata.org/entity/Q2")
    assert uri.n3(NS_MANAGER) == "wd:Q2"
    assert NS_MANAGER.expand_curie("wd:Q2") == uri

    uri = URIRef(
        "http://www.wikidata.org/entity/statement/"
        "Q2-a4078553-4ec1-a64a-79e7-c5b5e17b2782"
    )
    assert uri.n3(NS_MANAGER) == "wds:Q2-a4078553-4ec1-a64a-79e7-c5b5e17b2782"
    assert NS_MANAGER.expand_curie("wds:Q2-a4078553-4ec1-a64a-79e7-c5b5e17b2782") == uri

    uri = URIRef(
        "http://www.wikidata.org/value/87d0dc1c7847f19ac0f19be978015dfb202cf59a"
    )
    assert uri.n3(NS_MANAGER) == "wdv:87d0dc1c7847f19ac0f19be978015dfb202cf59a"
    assert (
        NS_MANAGER.expand_curie("wdv:87d0dc1c7847f19ac0f19be978015dfb202cf59a") == uri
    )

    uri = URIRef(
        "http://www.wikidata.org/reference/87d0dc1c7847f19ac0f19be978015dfb202cf59a"
    )
    assert uri.n3(NS_MANAGER) == "wdref:87d0dc1c7847f19ac0f19be978015dfb202cf59a"
    assert (
        NS_MANAGER.expand_curie("wdref:87d0dc1c7847f19ac0f19be978015dfb202cf59a") == uri
    )

    uri = URIRef("http://www.wikidata.org/prop/direct/P9")
    assert uri.n3(NS_MANAGER) == "wdt:P9"
    assert NS_MANAGER.expand_curie("wdt:P9") == uri

    uri = URIRef("http://www.wikidata.org/prop/P9")
    assert uri.n3(NS_MANAGER) == "p:P9"
    assert NS_MANAGER.expand_curie("p:P9") == uri

    uri = URIRef("http://www.wikidata.org/prop/statement/P8")
    assert uri.n3(NS_MANAGER) == "ps:P8"
    assert NS_MANAGER.expand_curie("ps:P8") == uri

    uri = URIRef("http://www.wikidata.org/prop/qualifier/P8")
    assert uri.n3(NS_MANAGER) == "pq:P8"
    assert NS_MANAGER.expand_curie("pq:P8") == uri

    uri = URIRef("http://wikiba.se/ontology#NormalRank")
    assert uri.n3(NS_MANAGER) == "wikibase:NormalRank"
    assert NS_MANAGER.expand_curie("wikibase:NormalRank") == uri
