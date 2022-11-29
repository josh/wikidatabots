# pyright: strict

from rdflib.term import URIRef

from wikidata import (
    WD,
    WDS,
    WIKIBASE,
    WIKIDATABOTS,
    NS_MANAGER,
    OntologyURIRef,
    WDSURIRef,
    WDURIRef,
    WikidatabotsURIRef,
    WikidataURIRef,
    qid,
)


def test_qid():
    assert qid("Q42")


def test_wdref():
    wd_uri_ref = WDURIRef("http://www.wikidata.org/entity/Q42")
    assert isinstance(wd_uri_ref, str)
    assert isinstance(wd_uri_ref, URIRef)
    assert isinstance(wd_uri_ref, WDURIRef)
    assert str(wd_uri_ref) == "http://www.wikidata.org/entity/Q42"

    assert WDURIRef.namespace == WD
    assert WDURIRef.prefix == "wd"

    assert wd_uri_ref.n3() == "<http://www.wikidata.org/entity/Q42>"
    assert wd_uri_ref.local_name() == "Q42"
    assert wd_uri_ref.qname() == "wd:Q42"


def test_wdref_from_wikibase_ref():
    wd_uri_ref = WikidataURIRef("http://www.wikidata.org/entity/Q42")
    assert isinstance(wd_uri_ref, str)
    assert isinstance(wd_uri_ref, URIRef)
    assert isinstance(wd_uri_ref, WDURIRef)


def test_wdsref():
    wds_uri_ref = WDSURIRef(
        "http://www.wikidata.org/entity/statement/"
        "q172241-91B6C9F4-2F78-4577-9726-6E9D8D76B486"
    )
    assert isinstance(wds_uri_ref, str)
    assert isinstance(wds_uri_ref, URIRef)
    assert isinstance(wds_uri_ref, WDSURIRef)

    assert WDSURIRef.namespace == WDS
    assert WDSURIRef.prefix == "wds"

    assert wds_uri_ref.local_name() == "q172241-91B6C9F4-2F78-4577-9726-6E9D8D76B486"
    assert wds_uri_ref.qname() == "wds:q172241-91B6C9F4-2F78-4577-9726-6E9D8D76B486"


def test_wikidatabotsref():
    wdbots_uri_ref = WikidatabotsURIRef(
        "https://github.com/josh/wikidatabots#editSummary"
    )
    assert isinstance(wdbots_uri_ref, str)
    assert isinstance(wdbots_uri_ref, URIRef)
    assert isinstance(wdbots_uri_ref, WikidatabotsURIRef)

    assert WikidatabotsURIRef.namespace == WIKIDATABOTS
    assert WikidatabotsURIRef.prefix == "wikidatabots"


def test_ontology_rank():
    ontology_uri_ref = OntologyURIRef("http://wikiba.se/ontology#rank")
    assert isinstance(ontology_uri_ref, str)
    assert isinstance(ontology_uri_ref, URIRef)
    assert isinstance(ontology_uri_ref, OntologyURIRef)

    assert OntologyURIRef.namespace == WIKIBASE
    assert OntologyURIRef.prefix == "wikibase"

    assert ontology_uri_ref == OntologyURIRef(WIKIBASE.rank)
    assert ontology_uri_ref.local_name() == "rank"


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
