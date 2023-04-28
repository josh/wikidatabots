# pyright: strict

import time

from timeout import iter_until_deadline


def forever():
    while True:
        yield


def test_iter_until_deadline():
    deadline = time.time() + 0.1
    i = 0
    for _ in iter_until_deadline(forever(), deadline):
        i += 1
    assert i > 1000
