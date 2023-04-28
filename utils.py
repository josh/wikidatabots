# pyright: strict

import random
from collections.abc import Iterable, Sequence
from typing import Any, TypeVar

T = TypeVar("T")


def first(iterable: Iterable[T] | None) -> T | None:
    if not iterable:
        return None
    for el in iterable:
        return el
    return None


def shuffled(seq: Iterable[T]) -> Sequence[T]:
    lst = list(seq)
    random.shuffle(lst)
    return lst


def tryint(value: Any) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None
