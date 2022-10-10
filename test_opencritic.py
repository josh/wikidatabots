import opencritic


def test_fetch_game():
    game = opencritic.fetch_game(1548)
    assert game
    assert game["name"] == "The Legend of Zelda: Breath of the Wild"
    assert game["numReviews"] >= 172
