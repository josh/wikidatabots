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
