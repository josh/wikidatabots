# pyright: strict

import pytest

import opencritic


@pytest.mark.skipif(opencritic.RAPIDAPI_KEY is None, reason="Missing RAPIDAPI_KEY")
def test_fetch_game():
    game = opencritic.fetch_game(1548)
    assert game
    assert game["id"] == 1548
    assert game["name"] == "The Legend of Zelda: Breath of the Wild"
    assert (
        game["url"]
        == "https://opencritic.com/game/1548/the-legend-of-zelda-breath-of-the-wild"
    )
    assert game["numReviews"] >= 172
