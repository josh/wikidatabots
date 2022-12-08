# pyright: basic

import numpy as np
import numpy.ma as ma

from utils import batches, ma_reserve_capacity, np_reserve_capacity


def test_batches():
    assert list(batches([], 1)) == []
    assert list(batches([1, 2, 3], 1)) == [[1], [2], [3]]
    assert list(batches([1, 2, 3], 2)) == [[1, 2], [3]]
    assert list(batches([1, 2, 3], 3)) == [[1, 2, 3]]
    assert list(batches([1, 2, 3], 4)) == [[1, 2, 3]]


def test_np_reserve_capacity():
    x = np.array([1, 2, 3])

    x = np_reserve_capacity(x, size=5, fill_value=0)
    assert x.tolist() == [1, 2, 3, 0, 0]

    x = np_reserve_capacity(x, size=4, fill_value=0)
    assert x.tolist() == [1, 2, 3, 0, 0]


def test_ma_reserve_capacity():
    x = np.array([1, 2, 3])
    mx = ma.masked_array(x, mask=[0, 1, 0])

    mx = ma_reserve_capacity(mx, size=5)
    assert mx.tolist() == [1, None, 3, None, None]

    mx = ma_reserve_capacity(mx, size=4)
    assert mx.tolist() == [1, None, 3, None, None]
