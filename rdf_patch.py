# pyright: reportGeneralTypeIssues=false

import datetime
import logging
import os
from collections import defaultdict
from functools import cache
from typing import Any, Iterator, TextIO

import pywikibot
import pywikibot.config
from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager
from rdflib.term import BNode, Literal, URIRef

from actions import print_warning
from wikidata import page_qids

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


P = Namespace("http://www.wikidata.org/prop/")
PQ = Namespace("http://www.wikidata.org/prop/qualifier/")
PR = Namespace("http://www.wikidata.org/prop/reference/")
PS = Namespace("http://www.wikidata.org/prop/statement/")
PSV = Namespace("http://www.wikidata.org/prop/statement/value/")
PROV = Namespace("http://www.w3.org/ns/prov#")
WD = Namespace("http://www.wikidata.org/entity/")
WDREF = Namespace("http://www.wikidata.org/reference/")
WDS = Namespace("http://www.wikidata.org/entity/statement/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")
WDV = Namespace("http://www.wikidata.org/value/")
WDNO = Namespace("http://www.wikidata.org/prop/novalue/")
WIKIBASE = Namespace("http://wikiba.se/ontology#")

WIKIDATABOTS = Namespace("https://github.com/josh/wikidatabots#")

NS_MANAGER = NamespaceManager(Graph())
NS_MANAGER.bind("wikibase", WIKIBASE)
NS_MANAGER.bind("wd", WD)
NS_MANAGER.bind("wds", WDS)
NS_MANAGER.bind("wdv", WDV)
NS_MANAGER.bind("wdref", WDREF)
NS_MANAGER.bind("wdt", WDT)
NS_MANAGER.bind("p", P)
NS_MANAGER.bind("wdno", WDNO)
NS_MANAGER.bind("ps", PS)
NS_MANAGER.bind("psv", PSV)
NS_MANAGER.bind("pq", PQ)
NS_MANAGER.bind("pr", PR)

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
    pywikibot.config.maxlag = 10
    pywikibot.config.put_throttle = 0

    graph = Graph()
    data = PREFIXES + input.read()
    graph.parse(data=data)

    blocked_qids: set[str] = set(page_qids("User:Josh404Bot/Blocklist"))

    changed_claims: dict[pywikibot.ItemPage, set[HashableClaim]] = defaultdict(set)
    edit_summaries: dict[pywikibot.ItemPage, str] = {}

    def mark_changed(
        item: pywikibot.ItemPage, claim: pywikibot.Claim, did_change: bool = True
    ):
        if did_change:
            changed_claims[item].add(HashableClaim(claim))

    def visit_wd_subject(
        item: pywikibot.ItemPage, predicate: URIRef, object: AnyObject
    ) -> None:
        predicate_prefix, predicate_local_name = compute_qname(predicate)

        if predicate_prefix == "wdt":
            property: pywikibot.PropertyPage = get_property_page(predicate_local_name)
            target = object_to_target(object)
            did_change, claim = item_append_claim_target(item, property, target)
            mark_changed(item, claim, did_change)

        elif predicate_prefix == "p" and isinstance(object, BNode):
            property: pywikibot.PropertyPage = get_property_page(predicate_local_name)

            claim: pywikibot.Claim = property.newClaim()
            item.claims[predicate_local_name].append(claim)
            mark_changed(item, claim)

            for predicate, object in predicate_objects(graph, object):
                visit_wds_subject(item, claim, predicate, object)

        elif predicate == WIKIDATABOTS.editSummary:
            edit_summaries[item] = object.toPython()

        else:
            print_warning(
                "NotImplemented", f"Unknown wd triple: {subject} {predicate} {object}"
            )

    def visit_wds_subject(
        item: pywikibot.ItemPage,
        claim: pywikibot.Claim,
        predicate: URIRef,
        object: AnyObject,
    ) -> None:
        predicate_prefix, predicate_local_name = compute_qname(predicate)

        if predicate_prefix == "pq":
            property = get_property_page(predicate_local_name)

            if graph_empty_node(graph, object):
                if predicate_local_name in claim.qualifiers:
                    del claim.qualifiers[predicate_local_name]
                    mark_changed(item, claim, True)
            else:
                target = object_to_target(object)
                did_change, _ = claim_append_qualifer(claim, property, target)
                mark_changed(item, claim, did_change)

        elif predicate_prefix == "ps":
            property = get_property_page(predicate_local_name)
            target = object_to_target(object)
            assert claim.getID() == property.getID()

            if not claim.target_equals(target):
                claim.setTarget(target)
                mark_changed(item, claim)

        elif predicate == WIKIBASE.rank:
            assert isinstance(object, URIRef)
            did_change = claim_set_rank(claim, object)
            mark_changed(item, claim, did_change)

        elif predicate == PROV.wasDerivedFrom:
            assert isinstance(object, BNode)

            source = defaultdict(list)

            for predicate, object in predicate_objects(graph, object):
                predicate_prefix, predicate_local_name = compute_qname(predicate)
                assert predicate_prefix == "pr"
                property = get_property_page(predicate_local_name)
                reference_claim = property.newClaim(is_reference=True)
                reference_claim.setTarget(object_to_target(object))
                source[reference_claim.getID()].append(reference_claim)

            claim.sources.append(source)

        elif predicate == WIKIDATABOTS.editSummary:
            edit_summaries[item] = object.toPython()

        else:
            print_warning("NotImplemented", f"Unknown wds triple: {predicate} {object}")

    for subject in subjects(graph):
        if isinstance(subject, BNode):
            continue

        assert isinstance(subject, URIRef)
        prefix, local_name = compute_qname(subject)

        if prefix == "wd":
            assert isinstance(subject, URIRef)
            item: pywikibot.ItemPage = get_item_page(local_name)
            for predicate, object in predicate_objects(graph, subject):
                visit_wd_subject(item, predicate, object)

        elif prefix == "wds":
            assert isinstance(subject, URIRef)
            claim: pywikibot.Claim = resolve_claim_guid(local_name)
            claim_item: pywikibot.ItemPage | None = claim.on_item
            assert claim_item
            for predicate, object in predicate_objects(graph, subject):
                visit_wds_subject(claim_item, claim, predicate, object)

        else:
            print_warning("NotImplemented", f"Unknown subject: {subject}")

    for item, claims in changed_claims.items():
        if item.id in blocked_qids:
            print_warning("BadItem", f"Skipping edit, {item.id} is blocked")
            continue

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


AnySubject = URIRef | BNode
AnyPredicate = URIRef
AnyObject = URIRef | BNode | Literal


def subjects(graph: Graph) -> Iterator[AnySubject]:
    for subject in graph.subjects():
        assert isinstance(subject, URIRef) or isinstance(subject, BNode)
        yield subject


def predicate_objects(
    graph: Graph, subject: AnySubject
) -> Iterator[tuple[AnyPredicate, AnyObject]]:
    for predicate, object in graph.predicate_objects(subject):
        assert isinstance(predicate, URIRef)
        assert (
            isinstance(object, URIRef)
            or isinstance(object, BNode)
            or isinstance(object, Literal)
        )
        yield predicate, object


def graph_empty_node(graph: Graph, object: AnyObject) -> bool:
    return isinstance(object, BNode) and len(list(graph.predicate_objects(object))) == 0


def compute_qname(uri: URIRef) -> tuple[str, str]:
    prefix, _, name = NS_MANAGER.compute_qname(uri)
    return (prefix, name)


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
def resolve_claim_guid(guid: str) -> pywikibot.Claim:
    qid, hash = guid.split("-", 1)
    snak = f"{qid}${hash}"

    item = get_item_page(qid.upper())

    for property in item.claims:
        for claim in item.claims[property]:
            if snak == claim.snak:
                return claim

    assert False, f"Can't resolve statement GUID: {guid}"


def object_to_target(object: AnyObject) -> Any:
    if isinstance(object, Literal):
        value = object.toPython()
        if type(value) == datetime.date:
            return pywikibot.WbTime.fromTimestr(f"{object}T00:00:00Z", precision=11)
        else:
            return value
    elif isinstance(object, URIRef):
        prefix, local_name = compute_qname(object)
        assert prefix == "wd"
        return get_item_page(local_name)
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


def login(username: str, password: str) -> None:
    pywikibot.config.password_file = "user-password.py"
    with open(pywikibot.config.password_file, "w") as file:
        file.write(f'("{username}", "{password}")')
    os.chmod(pywikibot.config.password_file, 0o600)

    pywikibot.config.usernames["wikidata"]["wikidata"] = username

    site = pywikibot.Site("wikidata", "wikidata")
    site.login()


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Process Wikidata RDF changes.")
    parser.add_argument("-u", "--username", action="store")
    parser.add_argument("-p", "--password", action="store")
    parser.add_argument("-n", "--dry-run", action="store_true")
    args = parser.parse_args()

    username = (
        args.username
        or os.environ.get("QUICKSTATEMENTS_USERNAME")
        or os.environ.get("WIKIDATA_USERNAME")
    )
    password = args.password or os.environ.get("WIKIDATA_PASSWORD")

    if (not args.dry_run) and username and password:
        login(username, password)

    edits = process_graph(
        username=username,
        input=sys.stdin,
    )

    for item, claims, summary in edits:
        if args.dry_run:
            continue
        item.editEntity({"claims": claims}, summary=summary)
