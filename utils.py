import random
from collections.abc import Iterable, Iterator, Sequence
from typing import Any, Optional, TypeVar

T = TypeVar("T")


def batches(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    batch: list[T] = []

    for element in iterable:
        batch.append(element)
        if len(batch) == size:
            yield batch
            batch = []

    if batch:
        yield batch


def shuffled(seq: Iterable[T]) -> Sequence[T]:
    lst = list(seq)
    random.shuffle(lst)
    return lst


def position_weighted_shuffled(seq: Iterable[T]) -> list[T]:
    def weight(el: tuple[int, T]) -> float:
        return random.uniform(0, el[0])

    lst = list(enumerate(seq))
    lst.sort(key=weight)
    return list([el for _, el in lst])


def uniq(*lists: Iterable[T]) -> Iterator[T]:
    seen: list[T] = []
    for lst in lists:
        for el in lst:
            if el not in seen:
                yield el
                seen.append(el)


def tryint(value: Any) -> Optional[int]:
    try:
        return int(value)
    except ValueError:
        return None
