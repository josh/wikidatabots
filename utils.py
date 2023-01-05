import logging
import random
from collections.abc import Iterable, Iterator, Sequence
from typing import Any, TypeVar

import numpy as np
import numpy.ma as ma
import numpy.typing as npt

T = TypeVar("T")


def first(iterable: Iterable[T] | None) -> T | None:
    if not iterable:
        return None
    for el in iterable:
        return el
    return None


def batches(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    assert size > 0
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


def tryint(value: Any) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


T1 = TypeVar("T1", bound=npt.NBitBase)


def np_reserve_capacity(
    array: np.ndarray[T1, np.dtype[Any]],
    size: int,
    fill_value: Any,
) -> np.ndarray[T1, np.dtype[Any]]:
    if array.size >= size:
        return array
    logging.debug(f"Resizing ndarray {array.size}->{size}")
    new_array = np.full(size, fill_value=fill_value, dtype=array.dtype)
    new_array[: array.size] = array
    return new_array


def ma_reserve_capacity(
    array: np.ma.MaskedArray[T1, np.dtype[Any]],
    size: int,
) -> np.ma.MaskedArray[T1, np.dtype[Any]]:
    if array.size >= size:
        return array
    logging.debug(f"Resizing ndarray {array.size}->{size}")
    new_array = ma.masked_all(size, dtype=array.dtype)
    new_array[: array.size] = array
    return new_array
