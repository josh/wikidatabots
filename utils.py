import random
from typing import Iterable, Iterator, Sequence, TypeVar

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


def uniq(*lists: Iterable[T]) -> Iterator[T]:
    seen: list[T] = []
    for lst in lists:
        for el in lst:
            if el not in seen:
                yield el
                seen.append(el)
