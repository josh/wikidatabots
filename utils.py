# pyright: strict

from collections.abc import Iterable
from typing import Any, TypeVar

T = TypeVar("T")


def first(iterable: Iterable[T] | None) -> T | None:
    if not iterable:
        return None
    for el in iterable:
        return el
    return None


def tryint(value: Any) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None
