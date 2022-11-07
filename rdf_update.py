import logging
from typing import NewType, TextIO

import pywikibot
import pywikibot.config
from rdflib import Graph
from rdflib.term import URIRef

from wikidata import PQ, WD, WDS, WIKIBASE

Ontology = NewType("Ontology", str)
ResolvedURI = pywikibot.PropertyPage | pywikibot.ItemPage | pywikibot.Claim | Ontology

SITE = pywikibot.Site("wikidata", "wikidata")


def process_graph(username: str, input: TextIO) -> None:
    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    pywikibot.config.password_file = "user-password.py"

    graph = Graph()
    graph.parse(input)

    subjects: set[URIRef] = set()

    for subject, predicate, object in graph:
        assert isinstance(subject, URIRef)
        assert isinstance(predicate, URIRef)
        assert isinstance(object, URIRef)

        subjects.add(subject)

        if subject in WDS and predicate in PQ and object in WD:
            logging.debug("Processing statement property qualifier")
            claim = resolve_entity_statement(subject)
            property = resolve_property_qualifier(predicate)
            target = resolve_entity(object)

            qualifier = claim.qualifiers.get(property.id, [])[0]
            if not qualifier:
                qualifier = property.newClaim(isQualifier=True)
                claim.qualifiers[property.id] = [qualifier]
            qualifier.setTarget(target)

        elif subject in WDS and predicate == WIKIBASE.rank and object in WIKIBASE:
            logging.debug("Processing statement rank")
            claim = resolve_entity_statement(subject)
            claim_set_rank(claim, object)

        else:
            logging.error(f"Unknown triple: {subject} {predicate} {object}")

    for subject in subjects:
        if subject in WDS:
            claim = resolve_entity_statement(subject)
            item: pywikibot.ItemPage | None = claim.on_item
            assert item
            logging.info(f"Will update claim {claim.id} on item {item.id}")
        else:
            logging.error(f"Failed to save unknown subject: {subject}")


ITEM_CACHE: dict[URIRef, pywikibot.ItemPage] = {}
PROPERTY_CACHE: dict[URIRef, pywikibot.PropertyPage] = {}
CLAIM_CACHE: dict[URIRef, pywikibot.Claim] = {}


def resolve_entity(uri: URIRef) -> pywikibot.ItemPage:
    assert uri.startswith("http://www.wikidata.org/entity/Q")
    if uri in ITEM_CACHE:
        return ITEM_CACHE[uri]
    item = pywikibot.ItemPage.from_entity_uri(SITE, uri)
    ITEM_CACHE[uri] = item
    logging.debug(f"Loading item {item.getID()}")
    return item


def resolve_property_qualifier(uri: URIRef) -> pywikibot.PropertyPage:
    assert uri.startswith("http://www.wikidata.org/prop/qualifier/P")
    if uri in PROPERTY_CACHE:
        return PROPERTY_CACHE[uri]
    pid = uri.removeprefix("http://www.wikidata.org/prop/qualifier/")
    property = pywikibot.PropertyPage(SITE, pid)
    PROPERTY_CACHE[uri] = property
    logging.debug(f"Loading property {property.getID()}")
    return property


def resolve_entity_statement(uri: URIRef) -> pywikibot.Claim:
    assert uri.startswith("http://www.wikidata.org/entity/statement/Q")
    if uri in CLAIM_CACHE:
        return CLAIM_CACHE[uri]

    guid = uri.removeprefix("http://www.wikidata.org/entity/statement/")
    assert "$" not in guid
    assert guid.startswith("Q")
    qid, hash = guid.split("-", 1)
    snak = f"{qid}${hash}"

    item = resolve_entity(URIRef(f"http://www.wikidata.org/entity/{qid}"))

    for property in item.claims:
        for claim in item.claims[property]:
            if snak == claim.snak:
                return claim

    assert False, f"Can't resolve statement GUID: {uri}"


def claim_set_rank(claim: pywikibot.Claim, rank: URIRef) -> None:
    if rank == WIKIBASE.NormalRank:
        claim.setRank("normal")
    elif rank == WIKIBASE.DeprecatedRank:
        claim.setRank("deprecated")
    elif rank == WIKIBASE.PreferredRank:
        claim.setRank("preferred")
    else:
        assert False, f"Unknown rank: {rank}"


if __name__ == "__main__":
    import argparse
    import os
    import sys

    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description="Process Wikidata RDF changes.")
    parser.add_argument("--username", action="store")
    args = parser.parse_args()

    process_graph(
        username=args.username
        or os.environ.get("QUICKSTATEMENTS_USERNAME")
        or os.environ["WIKIDATA_USERNAME"],
        input=sys.stdin,
    )
