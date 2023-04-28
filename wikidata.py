# pyright: strict

import re
from typing import Any, NewType

from rdflib import Graph
from rdflib.namespace import Namespace, NamespaceManager

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
