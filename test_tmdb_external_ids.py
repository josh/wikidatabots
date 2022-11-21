# pyright: strict

import imdb
from tmdb_external_ids import fast_imdb_id_lookup


def test_fast_imdb_id_lookup():
    assert fast_imdb_id_lookup("movie", imdb.id("tt0111161")) is True
    assert fast_imdb_id_lookup("tv", imdb.id("tt0944947")) is True
    assert fast_imdb_id_lookup("person", imdb.id("nm0000151")) is True
