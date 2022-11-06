# pyright: strict

import re
from typing import Any, NewType

PID = NewType("PID", str)
QID = NewType("QID", str)

PIDPattern = re.compile("Q[1-9][0-9]*")
QIDPattern = re.compile("Q[1-9][0-9]*")


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
