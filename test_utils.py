from utils import batches, iter_with_timeout


def test_batches():
    assert list(batches([], 1)) == []
    assert list(batches([1, 2, 3], 1)) == [[1], [2], [3]]
    assert list(batches([1, 2, 3], 2)) == [[1, 2], [3]]
    assert list(batches([1, 2, 3], 3)) == [[1, 2, 3]]
    assert list(batches([1, 2, 3], 4)) == [[1, 2, 3]]


def test_iter_with_timeout():
    def forever():
        while True:
            yield True

    for i in iter_with_timeout(forever(), timeout=0.1):
        pass
