# pyright: reportGeneralTypeIssues=false

import datetime
import os
import sys
from collections import OrderedDict, defaultdict
from functools import cache
from typing import Any, Iterator, TextIO

import pywikibot  # type: ignore
import pywikibot.config  # type: ignore
from rdflib import XSD, Graph
from rdflib.namespace import Namespace, NamespaceManager
from rdflib.term import BNode, Literal, URIRef

from actions import print_warning
from wikidata import blocklist

SITE = pywikibot.Site("wikidata", "wikidata")


class HashableClaim:
    def __init__(self, claim: pywikibot.Claim):
        self.claim = claim

    def __eq__(self, other):
        if not isinstance(other, HashableClaim):
            return False
        return self.claim == other.claim

    def __hash__(self):
        return 0


P = Namespace("http://www.wikidata.org/prop/")
PQ = Namespace("http://www.wikidata.org/prop/qualifier/")
PQE = Namespace("http://www.wikidata.org/prop/qualifier/exclusive/")
PQV = Namespace("http://www.wikidata.org/prop/qualifier/value/")
PQVE = Namespace("http://www.wikidata.org/prop/qualifier/value-exclusive/")
PR = Namespace("http://www.wikidata.org/prop/reference/")
PRV = Namespace("http://www.wikidata.org/prop/reference/value/")
PS = Namespace("http://www.wikidata.org/prop/statement/")
PSV = Namespace("http://www.wikidata.org/prop/statement/value/")
PROV = Namespace("http://www.w3.org/ns/prov#")
WD = Namespace("http://www.wikidata.org/entity/")
WDREF = Namespace("http://www.wikidata.org/reference/")
WDS = Namespace("http://www.wikidata.org/entity/statement/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")
WDV = Namespace("http://www.wikidata.org/value/")
WDNO = Namespace("http://www.wikidata.org/prop/novalue/")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
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
NS_MANAGER.bind("pqe", PQE)
NS_MANAGER.bind("pqv", PQV)
NS_MANAGER.bind("pqve", PQVE)
NS_MANAGER.bind("pr", PR)
NS_MANAGER.bind("prv", PRV)
NS_MANAGER.bind("wikidatabots", WIKIDATABOTS)

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
PREFIX pqe: <http://www.wikidata.org/prop/qualifier/exclusive/>
PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>
PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>
PREFIX pqve: <http://www.wikidata.org/prop/qualifier/value-exclusive/>
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


AnyRDFSubject = URIRef | BNode
AnyRDFPredicate = URIRef
AnyRDFObject = URIRef | BNode | Literal


def _subjects(graph: Graph) -> Iterator[AnyRDFSubject]:
    for subject in graph.subjects(unique=True):
        assert isinstance(subject, URIRef) or isinstance(subject, BNode)
        yield subject


def _predicate_objects(
    graph: Graph, subject: AnyRDFSubject
) -> Iterator[tuple[AnyRDFPredicate, AnyRDFObject]]:
    for predicate, object in graph.predicate_objects(subject, unique=True):
        assert isinstance(predicate, URIRef)
        assert (
            isinstance(object, URIRef)
            or isinstance(object, BNode)
            or isinstance(object, Literal)
        )
        yield predicate, object


def _predicate_ns_objects(
    graph: Graph, subject: AnyRDFSubject, predicate_ns: Namespace
) -> Iterator[tuple[str, AnyRDFObject]]:
    for predicate, object in _predicate_objects(graph, subject):
        _, ns, name = NS_MANAGER.compute_qname(predicate)
        if predicate_ns == ns:
            yield name, object


class WbSource:
    _source: OrderedDict[str, list[pywikibot.Claim]]

    def __init__(self):
        self._source = OrderedDict()

    def add_reference(self, pid: str, reference: pywikibot.Claim) -> None:
        assert pid.startswith("P"), pid
        if pid not in self._source:
            self._source[pid] = []
        self._source[pid].append(reference)


def _compute_qname(uri: URIRef) -> tuple[str, str]:
    prefix, _, name = NS_MANAGER.compute_qname(uri)
    return (prefix, name)


@cache
def get_item_page(qid: str) -> pywikibot.ItemPage:
    assert qid.startswith("Q"), qid
    return pywikibot.ItemPage(SITE, qid)


@cache
def get_property_page(pid: str) -> pywikibot.PropertyPage:
    assert pid.startswith("P"), pid
    return pywikibot.PropertyPage(SITE, pid)


def _resolve_object_uriref(
    object: URIRef,
) -> pywikibot.ItemPage | pywikibot.PropertyPage:
    prefix, local_name = _compute_qname(object)
    assert prefix == "wd"
    if local_name.startswith("Q"):
        return get_item_page(local_name)
    elif local_name.startswith("P"):
        return get_property_page(local_name)
    else:
        raise NotImplementedError(f"Unknown item: {object}")


def _resolve_object_literal(object: Literal) -> str | pywikibot.WbTime:
    if object.datatype is None:
        return str(object)
    elif object.datatype == XSD.dateTime or object.datatype == XSD.date:
        data = {
            "time": object.toPython().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "precision": 11,
            "after": 0,
            "before": 0,
            "timezone": 0,
            "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
        }
        return pywikibot.WbTime.fromWikibase(data, site=SITE)
    else:
        raise NotImplementedError(f"not implemented datatype: {object.datatype}")


def _resolve_object(
    graph: Graph, object: AnyRDFObject
) -> (
    pywikibot.ItemPage
    | pywikibot.PropertyPage
    | pywikibot.WbTime
    | pywikibot.WbQuantity
    | WbSource
    | str
):
    if isinstance(object, URIRef):
        return _resolve_object_uriref(object)
    elif isinstance(object, BNode):
        return _resolve_object_bnode(graph, object)
    elif isinstance(object, Literal):
        return _resolve_object_literal(object)


def _resolve_object_bnode_time_value(graph: Graph, object: BNode) -> pywikibot.WbTime:
    if value := graph.value(object, WIKIBASE.timeValue):
        assert isinstance(value, Literal)
        assert value.datatype is None or value.datatype == XSD.dateTime
    if precision := graph.value(object, WIKIBASE.timePrecision):
        assert isinstance(precision, Literal)
        assert precision.datatype == XSD.integer
        assert 0 <= precision.toPython() <= 14
    if timezone := graph.value(object, WIKIBASE.timeTimezone):
        assert isinstance(timezone, Literal)
        assert timezone.datatype == XSD.integer
    if calendar_model := graph.value(object, WIKIBASE.timeCalendarModel):
        assert isinstance(calendar_model, URIRef)

    data = {
        "time": None,
        "precision": 11,
        "after": 0,
        "before": 0,
        "timezone": 0,
        "calendarmodel": "https://www.wikidata.org/wiki/Q1985727",
    }
    if value:
        value_dt = value.toPython()
        if not isinstance(value_dt, datetime.datetime):
            value_dt = datetime.datetime.fromisoformat(value_dt)
        data["time"] = value_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if precision:
        data["precision"] = precision.toPython()
    if timezone:
        data["timezone"] = timezone.toPython()
    if calendar_model:
        data["calendarmodel"] = str(calendar_model)
    return pywikibot.WbTime.fromWikibase(data, site=SITE)


def _resolve_object_bnode_quantity_value(
    graph: Graph, object: BNode
) -> pywikibot.WbQuantity:
    if amount := graph.value(object, WIKIBASE.quantityAmount):
        assert isinstance(amount, Literal)
        assert amount.datatype == XSD.decimal
    if upper_bound := graph.value(object, WIKIBASE.quantityUpperBound):
        assert isinstance(upper_bound, Literal)
        assert upper_bound.datatype == XSD.decimal
    if lower_bound := graph.value(object, WIKIBASE.quantityLowerBound):
        assert isinstance(lower_bound, Literal)
        assert lower_bound.datatype == XSD.decimal
    if unit := graph.value(object, WIKIBASE.quantityUnit):
        assert isinstance(unit, URIRef)

    data = {
        "amount": None,
        "upperBound": None,
        "lowerBound": None,
        "unit": "1",
    }
    if amount:
        data["amount"] = f"+{amount}"
    if upper_bound:
        data["upperBound"] = f"+{upper_bound}"
    if lower_bound:
        data["lowerBound"] = f"+{lower_bound}"
    if unit:
        data["unit"] = str(unit)
    return pywikibot.WbQuantity.fromWikibase(data, site=SITE)


def _resolve_object_bnode(
    graph: Graph, object: BNode, rdf_type: URIRef | None = None
) -> pywikibot.WbQuantity | pywikibot.WbTime | WbSource:
    if not rdf_type:
        rdf_type = graph.value(object, RDF.type)
    assert rdf_type is None or isinstance(rdf_type, URIRef)

    if rdf_type == WIKIBASE.TimeValue:
        return _resolve_object_bnode_time_value(graph, object)
    elif rdf_type == WIKIBASE.QuantityValue:
        return _resolve_object_bnode_quantity_value(graph, object)
    elif rdf_type == WIKIBASE.Reference:
        return _resolve_object_bnode_reference(graph, object)
    else:
        raise NotImplementedError(f"Unknown bnode: {rdf_type}")


def _resolve_object_bnode_reference(graph: Graph, object: BNode) -> WbSource:
    source = WbSource()

    for pr_name, pr_object in _predicate_ns_objects(graph, object, PR):
        ref = get_property_page(pr_name).newClaim(is_reference=True)
        ref.setTarget(_resolve_object(graph, pr_object))
        source.add_reference(pr_name, ref)

    for prv_name, prv_object in _predicate_ns_objects(graph, object, PRV):
        ref = get_property_page(prv_name).newClaim(is_reference=True)
        ref.setTarget(_resolve_object(graph, prv_object))
        source.add_reference(prv_name, ref)

    return source


def _graph_empty_node(graph: Graph, object: AnyRDFObject) -> bool:
    return isinstance(object, BNode) and len(list(graph.predicate_objects(object))) == 0


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


def _claim_uri(claim: pywikibot.Claim) -> str:
    snak: str = claim.snak
    guid = snak.replace("$", "-")
    return f"http://www.wikidata.org/entity/statement/{guid}"


def _item_append_claim_target(
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

    new_claim: pywikibot.Claim = property.newClaim()
    new_claim.setTarget(target)
    item.claims[pid].append(new_claim)

    return (True, new_claim)


def _claim_append_qualifer(
    claim: pywikibot.Claim,
    property: pywikibot.PropertyPage,
    target: Any,
) -> bool:
    assert not isinstance(target, URIRef), f"Pass target as ItemPage: {target}"
    assert not isinstance(target, Literal), f"Pass target as Python value: {target}"

    pid: str = property.id
    if pid not in claim.qualifiers:
        claim.qualifiers[pid] = []
    qualifiers = claim.qualifiers[pid]

    for qualifier in qualifiers:
        if qualifier.target_equals(target):
            return False

    new_qualifier: pywikibot.Claim = property.newClaim(is_qualifier=True)
    new_qualifier.setTarget(target)
    claim.qualifiers[pid].append(new_qualifier)

    return True


def _claim_set_qualifer(
    claim: pywikibot.Claim,
    property: pywikibot.PropertyPage,
    target: Any,
) -> bool:
    assert not isinstance(target, URIRef), f"Pass target as ItemPage: {target}"
    assert not isinstance(target, Literal), f"Pass target as Python value: {target}"

    pid: str = property.id
    if pid in claim.qualifiers and len(claim.qualifiers[pid]) == 1:
        qualifier: pywikibot.Claim = claim.qualifiers[pid][0]
        if qualifier.target_equals(target):
            return False

    new_qualifier: pywikibot.Claim = property.newClaim(is_qualifier=True)
    new_qualifier.setTarget(target)
    claim.qualifiers[pid] = [new_qualifier]

    return True


_RANKS: dict[str, str] = {
    str(WIKIBASE.NormalRank): "normal",
    str(WIKIBASE.DeprecatedRank): "deprecated",
    str(WIKIBASE.PreferredRank): "preferred",
}


def _claim_set_rank(claim: pywikibot.Claim, rank: URIRef) -> bool:
    rank_str: str = _RANKS[str(rank)]
    if claim.rank == rank_str:
        return False
    claim.setRank(rank_str)
    return True


def process_graph(
    username: str,
    input: TextIO,
    blocked_qids: set[str] = set(),
) -> Iterator[tuple[pywikibot.ItemPage, list[dict[str, Any]], str | None]]:
    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    pywikibot.config.password_file = "user-password.py"
    pywikibot.config.maxlag = 10
    pywikibot.config.put_throttle = 0

    graph = Graph()
    data = PREFIXES + input.read()
    graph.parse(data=data)

    changed_claims: dict[pywikibot.ItemPage, set[HashableClaim]] = defaultdict(set)
    edit_summaries: dict[pywikibot.ItemPage, str] = {}

    def mark_changed(
        item: pywikibot.ItemPage, claim: pywikibot.Claim, did_change: bool = True
    ) -> None:
        if did_change:
            changed_claims[item].add(HashableClaim(claim))

    def visit_wd_subject(
        item: pywikibot.ItemPage, predicate: URIRef, object: AnyRDFObject
    ) -> None:
        predicate_prefix, predicate_local_name = _compute_qname(predicate)

        if predicate_prefix == "wdt":
            wdt_property: pywikibot.PropertyPage = get_property_page(
                predicate_local_name
            )
            target = _resolve_object(graph, object)
            did_change, claim = _item_append_claim_target(item, wdt_property, target)
            if claim.rank == "deprecated":
                print_warning(
                    "DeprecatedClaim",
                    f"<{_claim_uri(claim)}> already exists, but is deprecated",
                )
            mark_changed(item, claim, did_change)

        elif predicate_prefix == "p" and isinstance(object, BNode):
            p_property: pywikibot.PropertyPage = get_property_page(predicate_local_name)

            property_claim: pywikibot.Claim = p_property.newClaim()
            if predicate_local_name not in item.claims:
                item.claims[predicate_local_name] = []
            item.claims[predicate_local_name].append(property_claim)
            mark_changed(item, property_claim)

            for predicate, p_object in _predicate_objects(graph, object):
                visit_wds_subject(item, property_claim, predicate, p_object)

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
        object: AnyRDFObject,
    ) -> None:
        predicate_prefix, predicate_local_name = _compute_qname(predicate)

        if predicate_prefix == "pq" or predicate_prefix == "pqv":
            property = get_property_page(predicate_local_name)
            target = _resolve_object(graph, object)
            did_change = _claim_append_qualifer(claim, property, target)
            mark_changed(item, claim, did_change)

        elif predicate_prefix == "pqe" or predicate_prefix == "pqve":
            property = get_property_page(predicate_local_name)

            if _graph_empty_node(graph, object):
                if predicate_local_name in claim.qualifiers:
                    del claim.qualifiers[predicate_local_name]
                    mark_changed(item, claim, True)
            else:
                target = _resolve_object(graph, object)
                did_change = _claim_set_qualifer(claim, property, target)
                mark_changed(item, claim, did_change)

        elif predicate_prefix == "ps":
            property = get_property_page(predicate_local_name)
            target = _resolve_object(graph, object)
            assert claim.getID() == property.getID()

            if not claim.target_equals(target):
                claim.setTarget(target)
                mark_changed(item, claim)

        elif predicate == WIKIBASE.rank:
            assert isinstance(object, URIRef)
            did_change = _claim_set_rank(claim, object)
            mark_changed(item, claim, did_change)

        elif predicate == PROV.wasDerivedFrom or predicate == PROV.wasOnlyDerivedFrom:
            assert isinstance(object, BNode)
            source = _resolve_object_bnode_reference(graph, object)
            prev_sources = claim.sources.copy()
            if predicate == PROV.wasOnlyDerivedFrom:
                claim.sources = [source._source]
            else:
                claim.sources.append(source._source)
            mark_changed(item, claim, claim.sources != prev_sources)

        elif predicate == WIKIDATABOTS.editSummary:
            edit_summaries[item] = object.toPython()

        else:
            print_warning("NotImplemented", f"Unknown wds triple: {predicate} {object}")

    for subject in _subjects(graph):
        if isinstance(subject, BNode):
            continue

        assert isinstance(subject, URIRef)
        prefix, local_name = _compute_qname(subject)

        if prefix == "wd":
            assert isinstance(subject, URIRef)
            item: pywikibot.ItemPage = get_item_page(local_name)
            for predicate, object in _predicate_objects(graph, subject):
                visit_wd_subject(item, predicate, object)

        elif prefix == "wds":
            assert isinstance(subject, URIRef)
            claim: pywikibot.Claim = resolve_claim_guid(local_name)
            claim_item: pywikibot.ItemPage | None = claim.on_item
            assert claim_item
            for predicate, object in _predicate_objects(graph, subject):
                visit_wds_subject(claim_item, claim, predicate, object)

        elif subject == WIKIDATABOTS.testSubject:
            assert isinstance(subject, URIRef)
            for object in graph.objects(subject, WIKIDATABOTS.assertValue):
                assert _resolve_object(graph, object)

        else:
            print_warning("NotImplemented", f"Unknown subject: {subject}")

    for item, claims in changed_claims.items():
        if item.id in blocked_qids:
            print_warning("BadItem", f"Skipping edit, {item.id} is blocked")
            continue

        summary: str | None = edit_summaries.get(item)
        print(f"Edit {item.id}: {summary}", file=sys.stderr)

        claims_json: list[dict[str, Any]] = []
        for hclaim in claims:
            changed_claim: pywikibot.Claim = hclaim.claim
            claim_json: dict[str, Any] = changed_claim.toJSON()
            assert claim_json, "Claim had serialization error"
            claims_json.append(claim_json)
            print(
                f" ⮑ {changed_claim.id} / {changed_claim.snak or '(new claim)'}",
                file=sys.stderr,
            )

        assert len(claims_json) > 0, "No claims to save"
        yield (item, claims_json, summary)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process Wikidata RDF changes.")
    parser.add_argument("-n", "--dry-run", action="store_true")
    args = parser.parse_args()

    username = os.environ["WIKIDATA_USERNAME"]
    password = os.environ["WIKIDATA_PASSWORD"]

    if not args.dry_run:
        pywikibot.config.password_file = "user-password.py"
        with open(pywikibot.config.password_file, "w") as file:
            file.write(f'("{username}", "{password}")')
        os.chmod(pywikibot.config.password_file, 0o600)

        pywikibot.config.usernames["wikidata"]["wikidata"] = username

        SITE.login()

    blocked_qids = blocklist()

    edits = process_graph(
        username=username,
        input=sys.stdin,
        blocked_qids=blocked_qids,
    )

    for item, claims, summary in edits:
        if args.dry_run:
            continue
        item.editEntity({"claims": claims}, summary=summary)
