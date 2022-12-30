# pyright: strict

import appletv


def test_parse_movie_url():
    id = appletv.parse_movie_url(
        "https://tv.apple.com/us/movie/umc.cmc.459n4f98t82t8ommdoa7ebnny"
    )
    assert id == appletv.id("umc.cmc.459n4f98t82t8ommdoa7ebnny")

    id = appletv.parse_movie_url(
        "https://tv.apple.com/us/movie/top-gun-maverick/"
        "umc.cmc.670544bajp6s4pysx4rvctczz"
    )
    assert id == appletv.id("umc.cmc.670544bajp6s4pysx4rvctczz")

    id = appletv.parse_movie_url(
        "https://tv.apple.com/us/person/tom-cruise/umc.cpc.2eayi4unvl6pjq6kneojy16dk"
    )
    assert id is None


def test_appletv_to_itunes():
    itunes_id = appletv.appletv_to_itunes(
        appletv.id("umc.cmc.459n4f98t82t8ommdoa7ebnny")
    )
    assert itunes_id == 282875479
