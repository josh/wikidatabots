# pyright: strict

import logging
import re
from typing import Any, NewType

from rdflib import Namespace
from rdflib.term import URIRef

P = Namespace("http://www.wikidata.org/prop/")
PQ = Namespace("http://www.wikidata.org/prop/qualifier/")
PR = Namespace("http://www.wikidata.org/prop/reference/")
PS = Namespace("http://www.wikidata.org/prop/statement/")
PSN = Namespace("http://www.wikidata.org/prop/statement/value-normalized/")
PSV = Namespace("http://www.wikidata.org/prop/statement/value/")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SCHEMA = Namespace("http://schema.org/")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
WD = Namespace("http://www.wikidata.org/entity/")
WDREF = Namespace("http://www.wikidata.org/reference/")
WDS = Namespace("http://www.wikidata.org/entity/statement/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")
WDTN = Namespace("http://www.wikidata.org/prop/direct-normalized/")
WDV = Namespace("http://www.wikidata.org/value/")
WIKIBASE = Namespace("http://wikiba.se/ontology#")
GENID = Namespace("http://www.wikidata.org/.well-known/genid/")

WIKIDATABOTS = Namespace("https://github.com/josh/wikidatabots#")

PID = NewType("PID", str)
QID = NewType("QID", str)

PIDPattern = re.compile("Q[1-9][0-9]*")
QIDPattern = re.compile("Q[1-9][0-9]*")


def pid(id: Any) -> PID:
    assert type(id) is str, f"'{repr(id)}' is not a valid PID"
    assert re.fullmatch(PIDPattern, id), f"'{id}' is not a valid PID"
    return PID(id)


def qid(id: Any) -> QID:
    assert type(id) is str, f"'{repr(id)}' is not a valid QID"
    assert re.fullmatch(QIDPattern, id), f"'{id}' is not a valid QID"
    return QID(id)


def parse_uriref(uri: str) -> URIRef:
    if uri.startswith(WDS):
        return WDSURIRef(uri)
    elif uri.startswith(WDTN):
        return WDTNURIRef(uri)
    elif uri.startswith(WDT):
        return WDTURIRef(uri)
    elif uri.startswith(WD):
        return WDURIRef(uri)
    elif uri.startswith(PS):
        return PSURIRef(uri)
    elif uri.startswith(PQ):
        return PQURIRef(uri)
    elif uri.startswith(P):
        return PURIRef(uri)
    elif uri.startswith(WIKIDATABOTS):
        return WikidatabotsURIRef(uri)
    elif uri.startswith(WIKIBASE):
        return OntologyURIRef(uri)
    elif uri.startswith(GENID):
        return GenidURIRef(uri)
    else:
        logging.info(f"Unknown subtype for URIRef: {uri}")
        return URIRef(uri)


class WikidataURIRef(URIRef):
    prefix: str
    namespace: Namespace

    def __new__(cls, value: str) -> "WikidataURIRef":
        if cls == WikidataURIRef:
            uri = parse_uriref(value)
            assert isinstance(uri, WikidataURIRef), f"Invalid WikidataURIRef: {value}"
            return uri

        return str.__new__(cls, value)

    def __init__(self, value: str):
        assert value.startswith(self.namespace)
        URIRef.__init__(self)
        assert "/" not in self.local_name()

    def local_name(self) -> str:
        start = len(self.namespace)
        return self[start:]

    def qname(self) -> str:
        return f"{self.prefix}:{self.local_name()}"


class PURIRef(WikidataURIRef):
    prefix: str = "p"
    namespace: Namespace = P


class PQURIRef(WikidataURIRef):
    prefix: str = "pq"
    namespace: Namespace = PQ


class PSURIRef(WikidataURIRef):
    prefix: str = "ps"
    namespace: Namespace = PS


class WDURIRef(WikidataURIRef):
    prefix: str = "wd"
    namespace: Namespace = WD


class WDSURIRef(WikidataURIRef):
    prefix: str = "wds"
    namespace: Namespace = WDS


class WDTURIRef(WikidataURIRef):
    prefix: str = "wdt"
    namespace: Namespace = WDT


class WDTNURIRef(WikidataURIRef):
    prefix: str = "wdtn"
    namespace: Namespace = WDTN


class WikidatabotsURIRef(WikidataURIRef):
    prefix: str = "wikidatabots"
    namespace: Namespace = WIKIDATABOTS


class OntologyURIRef(WikidataURIRef):
    prefix: str = "wikibase"
    namespace: Namespace = WIKIBASE


class GenidURIRef(WikidataURIRef):
    prefix: str = "genid"
    namespace: Namespace = GENID
