import logging
from collections import defaultdict
from functools import cache
from typing import Any, Iterator, TextIO

import pywikibot
import pywikibot.config
from rdflib import Graph
from rdflib.term import BNode, Literal, URIRef

from wikidata import (
    PS,
    PSN,
    PSV,
    RDF,
    RDFS,
    SCHEMA,
    SKOS,
    WIKIBASE,
    WIKIDATABOTS,
    OntologyURIRef,
    PQURIRef,
    PURIRef,
    WDSURIRef,
    WDTNURIRef,
    WDTURIRef,
    WDURIRef,
    WikidatabotsURIRef,
    parse_uriref,
)

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
        elif isinstance(subject, WDSURIRef):
            visit_wds_subject(subject, predicate, object)
        elif isinstance(subject, WDURIRef):
            visit_wd_subject(subject, predicate, object)
        else:
            logging.warning(f"Unknown triple: {subject} {predicate} {object}")

    def visit_wd_subject(
        subject: WDURIRef,
        predicate: URIRef,
        object: URIRef | BNode | Literal,
    ) -> None:
        subject_id = subject.local_name()

        if predicate in WIKIBASE:
            pass

        elif subject_id.startswith("Q") and isinstance(predicate, WDTNURIRef):
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif subject_id.startswith("Q") and isinstance(predicate, WDTURIRef):
            item = get_item_page(subject_id)
            pid = predicate.local_name()
            property = get_property_page(pid)
            target = object_to_target(object)

            did_change, claim = item_append_claim_target(item, property, target)
            if did_change:
                changed_claims[item].add(HashableClaim(claim))

        elif (
            subject_id.startswith("Q")
            and isinstance(predicate, PURIRef)
            and isinstance(object, WDSURIRef)
        ):
            logging.error(f"Unimplemented wd triple: {subject} {predicate} {object}")

        elif (
            subject_id.startswith("Q")
            and isinstance(predicate, PURIRef)
            and isinstance(object, BNode)
        ):
            item: pywikibot.ItemPage = get_item_page(subject_id)
            pid = predicate.local_name()
            property: pywikibot.PropertyPage = get_property_page(pid)
            predicate_statement_uri = PS[pid]

            for object2 in graph.objects(object, predicate_statement_uri):
                assert isinstance(object2, Literal) or isinstance(object2, URIRef)
                did_change, claim = item_append_claim_target(
                    item, property, object_to_target(object2)
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

        elif predicate == WikidatabotsURIRef(WIKIDATABOTS.editSummary):
            item: pywikibot.ItemPage = get_item_page(subject.local_name())
            edit_summaries[item] = object.toPython()

        else:
            logging.warning(f"Unknown wd triple: {subject} {predicate} {object}")

    def visit_wds_subject(
        subject: WDSURIRef,
        predicate: URIRef,
        object: URIRef | BNode | Literal,
    ) -> None:
        claim: pywikibot.Claim = resolve_entity_statement(subject)
        item: pywikibot.ItemPage | None = claim.on_item
        assert item

        if predicate in PSN:
            logging.error(f"Unimplemented wds triple: {subject} {predicate} {object}")

        elif predicate in PSV:
            logging.error(f"Unimplemented wds triple: {subject} {predicate} {object}")

        elif predicate in PS:
            logging.error(f"Unimplemented wds triple: {subject} {predicate} {object}")

        elif isinstance(predicate, PQURIRef):
            pid = predicate.local_name()
            property = get_property_page(pid)
            target = object_to_target(object)

            did_change, _ = claim_append_qualifer(claim, property, target)
            if did_change:
                changed_claims[item].add(HashableClaim(claim))

        elif (
            isinstance(predicate, OntologyURIRef)
            and predicate.local_name() == "rank"
            and isinstance(object, OntologyURIRef)
        ):
            if claim_set_rank(claim, object):
                changed_claims[item].add(HashableClaim(claim))

        elif predicate == WikidatabotsURIRef(WIKIDATABOTS.editSummary):
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

        if isinstance(subject, URIRef):
            subject = parse_uriref(subject)
        if isinstance(predicate, URIRef):
            predicate = parse_uriref(predicate)
        if isinstance(object, URIRef):
            object = parse_uriref(object)

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
def resolve_entity_statement(uri: WDSURIRef) -> pywikibot.Claim:
    qid, hash = uri.local_name().split("-", 1)
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
    elif isinstance(object, WDURIRef):
        return get_item_page(object.local_name())
    else:
        assert False, f"Can't convert object to target: {object}"


def item_append_claim_target(
    item: pywikibot.ItemPage,
    property: pywikibot.PropertyPage,
    target: Any,
) -> tuple[bool, pywikibot.Claim]:
    assert not isinstance(target, URIRef), f"Pass target as ItemPage: {target}"
    assert not isinstance(target, Literal), f"Pass target as Python value: {target}"

    pid: str = property.id
    if pid not in item.claims:
        item.claims[pid] = []
    claims = item.claims[pid]

    for claim in claims:
        if claim.target_equals(target):
            return (False, claim)

    claim: pywikibot.Claim = property.newClaim()
    claim.setTarget(target)
    item.claims[pid].append(claim)

    return (True, claim)


def claim_append_qualifer(
    claim: pywikibot.Claim,
    property: pywikibot.PropertyPage,
    target: Any,
) -> tuple[bool, pywikibot.Claim]:
    assert not isinstance(target, URIRef), f"Pass target as ItemPage: {target}"
    assert not isinstance(target, Literal), f"Pass target as Python value: {target}"

    pid: str = property.id
    if pid not in claim.qualifiers:
        claim.qualifiers[pid] = []
    qualifiers = claim.qualifiers[pid]

    for qualifier in qualifiers:
        if qualifier.target_equals(target):
            return (False, qualifier)

    qualifier: pywikibot.Claim = property.newClaim(is_qualifier=True)
    qualifier.setTarget(target)
    claim.qualifiers[pid].append(qualifier)

    return (True, qualifier)


RANKS: dict[str, str] = {
    str(WIKIBASE.NormalRank): "normal",
    str(WIKIBASE.DeprecatedRank): "deprecated",
    str(WIKIBASE.PreferredRank): "preferred",
}


def claim_set_rank(claim: pywikibot.Claim, rank: URIRef) -> bool:
    rank_str: str = RANKS[str(rank)]
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
