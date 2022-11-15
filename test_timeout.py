# pyright: strict

import time

from timeout import iter_until_deadline, iter_with_timeout


def forever():
    while True:
        yield


def test_iter_with_timeout():
    i = 0
    for _ in iter_with_timeout(forever(), timeout=0.1):
        i += 1
    assert i > 1000


def test_iter_until_deadline():
    deadline = time.time() + 0.1
    i = 0
    for _ in iter_until_deadline(forever(), deadline):
        i += 1
    assert i > 1000
