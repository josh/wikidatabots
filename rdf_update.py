import logging
from functools import cache
from typing import NewType, TextIO

import pywikibot
import pywikibot.config
from rdflib import Graph
from rdflib.term import BNode, Literal, URIRef

from wikidata import (
    PQ,
    PS,
    PSN,
    PSV,
    RDF,
    RDFS,
    SCHEMA,
    SKOS,
    WD,
    WDS,
    WDT,
    WDTN,
    WIKIBASE,
    P,
)

Ontology = NewType("Ontology", str)
ResolvedURI = pywikibot.PropertyPage | pywikibot.ItemPage | pywikibot.Claim | Ontology

SITE = pywikibot.Site("wikidata", "wikidata")


def process_graph(username: str, input: TextIO) -> None:
    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    pywikibot.config.password_file = "user-password.py"

    graph = Graph()
    graph.parse(input)

    bnodes: dict[BNode, tuple[URIRef, URIRef]] = {}
    changed_claims: dict[URIRef, pywikibot.Claim] = dict()

    def visit(
        subject: URIRef | BNode,
        predicate: URIRef,
        object: URIRef | BNode | Literal,
    ) -> None:
        if isinstance(subject, BNode):
            assert isinstance(object, URIRef)
            bnodes[subject] = (predicate, object)
        elif predicate == RDF.type:
            pass
        elif subject in WDS:
            visit_wds_subject(subject, predicate, object)
        elif subject in WD:
            visit_wd_subject(subject, predicate, object)
        else:
            logging.warning(f"Unknown triple: {subject} {predicate} {object}")

    def visit_wd_subject(
        subject: URIRef,
        predicate: URIRef,
        object: URIRef | BNode | Literal,
    ) -> None:
        subject_id = subject.removeprefix(WD)

        if predicate in WIKIBASE:
            pass

        elif subject_id.startswith("Q") and predicate in WDTN:
            _ = get_item_page(subject_id)
            pid = predicate.removeprefix(WDTN)
            _ = get_property_page(pid)
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif subject_id.startswith("Q") and predicate in WDT:
            _ = get_item_page(subject_id)
            pid = predicate.removeprefix(WDT)
            _ = get_property_page(pid)
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif subject_id.startswith("Q") and predicate in P and object in WDS:
            _ = get_item_page(subject_id)
            pid = predicate.removeprefix(P)
            _ = get_property_page(pid)
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif predicate == SCHEMA.name and isinstance(object, Literal):
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif predicate == SCHEMA.description and isinstance(object, Literal):
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif predicate == RDFS.label and isinstance(object, Literal):
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif predicate == SKOS.altLabel and isinstance(object, Literal):
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif predicate == SKOS.prefLabel and isinstance(object, Literal):
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        else:
            logging.warning(f"Unknown wd triple: {subject} {predicate} {object}")

    def visit_wds_subject(
        subject: URIRef,
        predicate: URIRef,
        object: URIRef | BNode | Literal,
    ) -> None:
        claim = resolve_entity_statement(subject)
        changed_claims[subject] = claim

        if predicate in PSN:
            pid = predicate.removeprefix(PSN)
            property = get_property_page(pid)
            logging.error(f"Unimplemented wds triple: {subject} {predicate} {object}")

        elif predicate in PSV:
            pid = predicate.removeprefix(PSV)
            property = get_property_page(pid)
            logging.error(f"Unimplemented wds triple: {subject} {predicate} {object}")

        elif predicate in PS:
            pid = predicate.removeprefix(PS)
            property = get_property_page(pid)
            logging.error(f"Unimplemented wds triple: {subject} {predicate} {object}")

        elif predicate in PQ and isinstance(object, URIRef) and object in WD:
            pid = predicate.removeprefix(PQ)
            property = get_property_page(pid)
            target = resolve_entity(object)

            qualifier = claim.qualifiers.get(property.id, [])[0]
            if not qualifier:
                qualifier = property.newClaim(isQualifier=True)
                claim.qualifiers[property.id] = [qualifier]
            qualifier.setTarget(target)

        elif (
            predicate == WIKIBASE.rank
            and isinstance(object, URIRef)
            and object in WIKIBASE
        ):
            claim_set_rank(claim, object)

        else:
            logging.warning(f"Unknown wds triple: {subject} {predicate} {object}")

    for subject, predicate, object in graph:
        assert isinstance(subject, URIRef) or isinstance(subject, BNode)
        assert isinstance(predicate, URIRef)
        assert (
            isinstance(object, URIRef)
            or isinstance(object, BNode)
            or isinstance(object, Literal)
        )

        visit(subject, predicate, object)

    for claim in changed_claims.values():
        item: pywikibot.ItemPage | None = claim.on_item
        assert item, "Claim is not on an item"
        logging.info(f"Will update claim {claim.id} on item {item.id}")


@cache
def get_item_page(qid: str) -> pywikibot.ItemPage:
    assert qid.startswith("Q"), qid
    logging.debug(f"Load item page: {qid}")
    return pywikibot.ItemPage(SITE, qid)


@cache
def get_property_page(pid: str) -> pywikibot.PropertyPage:
    assert pid.startswith("P"), pid
    logging.debug(f"Load property page: {pid}")
    return pywikibot.PropertyPage(SITE, pid)


@cache
def resolve_entity(uri: URIRef) -> pywikibot.ItemPage:
    assert uri.startswith("http://www.wikidata.org/entity/Q"), uri
    qid = uri.removeprefix("http://www.wikidata.org/entity/")
    return get_item_page(qid)


@cache
def resolve_entity_statement(uri: URIRef) -> pywikibot.Claim:
    assert uri.startswith("http://www.wikidata.org/entity/statement/"), uri
    guid = uri.removeprefix("http://www.wikidata.org/entity/statement/")
    assert "$" not in guid
    qid, hash = guid.split("-", 1)
    snak = f"{qid}${hash}"

    item = get_item_page(qid.upper())

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

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Process Wikidata RDF changes.")
    parser.add_argument("--username", action="store")
    args = parser.parse_args()

    process_graph(
        username=args.username
        or os.environ.get("QUICKSTATEMENTS_USERNAME")
        or os.environ["WIKIDATA_USERNAME"],
        input=sys.stdin,
    )
