import random


def batches(iterable, size):
    batch = []

    for element in iterable:
        batch.append(element)
        if len(batch) == size:
            yield batch
            batch = []

    if batch:
        yield batch


def shuffled(seq):
    lst = list(seq)
    random.shuffle(lst)
    return lst


def uniq(*lists):
    seen = []
    for lst in lists:
        for el in lst:
            if el not in seen:
                yield el
                seen.append(el)
