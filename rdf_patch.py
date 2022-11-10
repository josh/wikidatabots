import logging
from collections import defaultdict
from functools import cache
from typing import Any, Iterator, NewType, TextIO

import pywikibot
import pywikibot.config
from rdflib import Graph, Namespace
from rdflib.term import BNode, Literal, URIRef

from utils import first
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

SCRIPT_NS = Namespace("https://github.com/josh/wikidatabots#")

Ontology = NewType("Ontology", str)
ResolvedURI = pywikibot.PropertyPage | pywikibot.ItemPage | pywikibot.Claim | Ontology

SITE = pywikibot.Site("wikidata", "wikidata")


class HashableClaim(object):
    def __init__(self, claim: pywikibot.Claim):
        self.claim = claim

    def __hash__(self):
        return 0

    def __eq__(self, other):
        if not isinstance(other, HashableClaim):
            return False
        return self.claim == other.claim


PREFIXES = """
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX cc: <http://creativecommons.org/ns#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX hint: <http://www.bigdata.com/queryHints#>
PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX p: <http://www.wikidata.org/prop/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>
PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>
PREFIX pr: <http://www.wikidata.org/prop/reference/>
PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>
PREFIX prv: <http://www.wikidata.org/prop/reference/value/>
PREFIX psv: <http://www.wikidata.org/prop/statement/value/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdata: <http://www.wikidata.org/wiki/Special:EntityData/>
PREFIX wdno: <http://www.wikidata.org/prop/novalue/>
PREFIX wdref: <http://www.wikidata.org/reference/>
PREFIX wds: <http://www.wikidata.org/entity/statement/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/>
PREFIX wdv: <http://www.wikidata.org/value/>
PREFIX wikibase: <http://wikiba.se/ontology#>

PREFIX wikidatabots: <https://github.com/josh/wikidatabots#>

"""


def process_graph(
    username: str, input: TextIO
) -> Iterator[tuple[pywikibot.ItemPage, list[dict[str, Any]], str | None]]:
    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    pywikibot.config.password_file = "user-password.py"
    pywikibot.config.put_throttle = 0

    graph = Graph()
    data = PREFIXES + input.read()
    graph.parse(data=data)

    changed_claims: dict[pywikibot.ItemPage, set[HashableClaim]] = defaultdict(set)
    edit_summaries: dict[pywikibot.ItemPage, str] = {}

    def visit(
        subject: URIRef | BNode,
        predicate: URIRef,
        object: URIRef | BNode | Literal,
    ) -> None:
        if isinstance(subject, BNode):
            assert isinstance(object, URIRef) or isinstance(object, Literal)
            pass
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
            item = get_item_page(subject_id)
            pid = predicate.removeprefix(WDT)
            property = get_property_page(pid)
            target = object_to_target(object)

            did_change, claim = item_append_claim_target(item, property, target)
            if did_change:
                changed_claims[item].add(HashableClaim(claim))

        elif subject_id.startswith("Q") and predicate in P and object in WDS:
            _ = get_item_page(subject_id)
            pid = predicate.removeprefix(P)
            _ = get_property_page(pid)
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif (
            subject_id.startswith("Q") and predicate in P and isinstance(object, BNode)
        ):
            item: pywikibot.ItemPage = get_item_page(subject_id)
            pid = predicate.removeprefix(P)
            assert pid.startswith("P")
            property: pywikibot.PropertyPage = get_property_page(pid)
            predicate_statement_uri = PS[pid]

            for object2 in graph.objects(object, predicate_statement_uri):
                assert isinstance(object2, Literal)
                did_change, claim = item_append_claim_target(
                    item, property, object2.toPython()
                )
                if did_change:
                    changed_claims[item].add(HashableClaim(claim))

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

        elif predicate == SCRIPT_NS.editSummary:
            item: pywikibot.ItemPage = resolve_entity(subject)
            edit_summaries[item] = object.toPython()

        else:
            logging.warning(f"Unknown wd triple: {subject} {predicate} {object}")

    def visit_wds_subject(
        subject: URIRef,
        predicate: URIRef,
        object: URIRef | BNode | Literal,
    ) -> None:
        claim: pywikibot.Claim = resolve_entity_statement(subject)
        item: pywikibot.ItemPage | None = claim.on_item
        assert item

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

            # TODO: Figure out how to handle appending additional qualifier
            qualifier: pywikibot.Claim | None = first(claim.qualifiers.get(property.id))
            if not qualifier:
                qualifier = property.newClaim(is_qualifier=True)
                claim.qualifiers[property.id] = [qualifier]
                changed_claims[item].add(HashableClaim(claim))
            assert qualifier

            if not qualifier.target_equals(target):
                qualifier.setTarget(target)
                changed_claims[item].add(HashableClaim(claim))

        elif (
            predicate == WIKIBASE.rank
            and isinstance(object, URIRef)
            and object in WIKIBASE
        ):
            if claim_set_rank(claim, object):
                changed_claims[item].add(HashableClaim(claim))

        elif predicate == SCRIPT_NS.editSummary:
            edit_summaries[item] = object.toPython()

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

    for item, claims in changed_claims.items():
        summary: str | None = edit_summaries.get(item)
        logging.info(f"Edit {item.id}: {summary}")

        claims_json: list[dict[str, Any]] = []
        for hclaim in claims:
            claim: pywikibot.Claim = hclaim.claim
            claim_json: dict[str, Any] = claim.toJSON()
            assert claim_json, "Claim had serialization error"
            claims_json.append(claim_json)
            logging.info(f" ⮑ {claim.id} / {claim.snak or '(new claim)'}")

        assert len(claims_json) > 0, "No claims to save"
        yield (item, claims_json, summary)


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


def object_to_target(object: URIRef | BNode | Literal) -> Any:
    if isinstance(object, Literal):
        return object.toPython()
    elif isinstance(object, URIRef):
        return resolve_entity(object)
    else:
        assert False, f"Can't convert object to target: {object}"


def item_append_claim_target(
    item: pywikibot.ItemPage,
    property: pywikibot.PropertyPage,
    target: Any,
) -> tuple[bool, pywikibot.Claim]:
    assert not isinstance(target, Literal), f"Pass target as Python value: {target}"
    existing_claims = item.claims.get(property.id, [])
    for claim in existing_claims:
        if claim.target_equals(target):
            return (False, claim)

    claim: pywikibot.Claim = property.newClaim()
    claim.setTarget(target)
    if not item.claims.get(property.id):
        item.claims[property.id] = []
    item.claims[property.id].append(claim)
    return (True, claim)


RANKS: dict[URIRef, str] = {
    WIKIBASE.NormalRank: "normal",
    WIKIBASE.DeprecatedRank: "deprecated",
    WIKIBASE.PreferredRank: "preferred",
}


def claim_set_rank(claim: pywikibot.Claim, rank: URIRef) -> bool:
    rank_str: str = RANKS[rank]
    if claim.rank == rank_str:
        return False
    claim.setRank(rank_str)
    return True


if __name__ == "__main__":
    import argparse
    import os
    import sys

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Process Wikidata RDF changes.")
    parser.add_argument("-u", "--username", action="store")
    parser.add_argument("-n", "--dry-run", action="store_true")
    args = parser.parse_args()

    edits = process_graph(
        username=args.username
        or os.environ.get("QUICKSTATEMENTS_USERNAME")
        or os.environ["WIKIDATA_USERNAME"],
        input=sys.stdin,
    )

    for (item, claims, summary) in edits:
        if args.dry_run:
            continue
        item.editEntity({"claims": claims}, summary=summary)
