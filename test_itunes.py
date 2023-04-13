# pyright: strict


from itunes import batch_lookup


def test_batch_lookup_one() -> None:
    results = batch_lookup([285494571])
    (id, result) = list(results)[0]
    assert id == 285494571
    assert result
    assert result["trackName"] == "The Godfather"


def test_batch_lookup_miss() -> None:
    results = batch_lookup([200000])
    (id, result) = list(results)[0]
    assert id == 200000
    assert not result
