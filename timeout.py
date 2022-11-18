# pyright: strict

import time
from collections.abc import Iterable, Iterator
from typing import TypeVar

START_TIME = time.time()
DEFAULT_TIMEOUT = 10 * 60
DEFAULT_DEADLINE = START_TIME + DEFAULT_TIMEOUT

T = TypeVar("T")


def iter_until_deadline(
    seq: Iterable[T],
    deadline: float = DEFAULT_DEADLINE,
) -> Iterator[T]:
    for el in seq:
        yield el
        if time.time() >= deadline:
            break


def iter_with_timeout(seq: Iterable[T], timeout: float) -> Iterator[T]:
    deadline = time.time() + timeout
    return iter_until_deadline(seq, deadline)


def max_time() -> float:
    return max(DEFAULT_DEADLINE - time.time(), 0)
