# pyright: strict

from utils import batches

def test_batches():
    assert list(batches([], 1)) == []
    assert list(batches([1, 2, 3], 1)) == [[1], [2], [3]]
    assert list(batches([1, 2, 3], 2)) == [[1, 2], [3]]
    assert list(batches([1, 2, 3], 3)) == [[1, 2, 3]]
    assert list(batches([1, 2, 3], 4)) == [[1, 2, 3]]
