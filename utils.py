import random


def uniq(*lists):
    seen = []
    for lst in lists:
        for el in lst:
            if el not in seen:
                yield el
                seen.append(el)


def shuffled(seq):
    lst = list(seq)
    random.shuffle(lst)
    return lst
