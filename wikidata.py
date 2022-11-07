# pyright: strict

import re
from typing import Any, NewType

from rdflib import Namespace

WD = Namespace("http://www.wikidata.org/entity/")
WDS = Namespace("http://www.wikidata.org/entity/statement/")
WDV = Namespace("http://www.wikidata.org/value/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")
WIKIBASE = Namespace("http://wikiba.se/ontology#")
P = Namespace("http://www.wikidata.org/prop/")
PS = Namespace("http://www.wikidata.org/prop/statement/")
PQ = Namespace("http://www.wikidata.org/prop/qualifier/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

PID = NewType("PID", str)
QID = NewType("QID", str)
StatementGUID = NewType("StatementGUID", str)

PIDPattern = re.compile("Q[1-9][0-9]*")
QIDPattern = re.compile("Q[1-9][0-9]*")
StatementGUIDPattern = re.compile("[qQ][0-9]+\\$[0-9a-fA-F-]+")


def pid(id: Any) -> PID:
    assert type(id) is str, f"'{repr(id)}' is not a valid PID"
    assert re.fullmatch(PIDPattern, id), f"'{id}' is not a valid PID"
    return PID(id)


def trypid(id: Any) -> PID | None:
    if type(id) is str and re.fullmatch(PIDPattern, id):
        return PID(id)
    return None


def qid(id: Any) -> QID:
    assert type(id) is str, f"'{repr(id)}' is not a valid QID"
    assert re.fullmatch(QIDPattern, id), f"'{id}' is not a valid QID"
    return QID(id)


def tryqid(id: Any) -> QID | None:
    if type(id) is str and re.fullmatch(QIDPattern, id):
        return QID(id)
    return None


def statement(guid: Any) -> StatementGUID:
    assert type(guid) is str, f"'{repr(guid)}' is not a valid statement GUID"
    assert re.fullmatch(
        StatementGUIDPattern, guid
    ), f"'{guid}' is not a valid statement GUID"
    return StatementGUID(guid)


def trystatement(guid: Any) -> StatementGUID | None:
    if type(guid) is str and re.fullmatch(StatementGUIDPattern, guid):
        return StatementGUID(guid)
    return None
