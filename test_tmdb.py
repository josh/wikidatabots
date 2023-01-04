# pyright: strict

import pytest

import tmdb


@pytest.mark.skipif(tmdb.TMDB_API_KEY is None, reason="Missing TMDB_API_KEY")
def test_find():
    result = tmdb.find(id="tt0111161", source="imdb_id", type="movie")
    assert result
    assert result["id"] == 278
    assert result["title"] == "The Shawshank Redemption"

    result = tmdb.find(id="/m/0479b", source="freebase_mid", type="person")
    assert result
    assert result["id"] == 6384

    assert not tmdb.find(id="tt10000000000", source="imdb_id", type="movie")


@pytest.mark.skipif(tmdb.TMDB_API_KEY is None, reason="Missing TMDB_API_KEY")
def test_external_ids():
    ids = tmdb.external_ids(id=278, type="movie")
    assert ids["imdb_id"] == "tt0111161"


@pytest.mark.skipif(tmdb.TMDB_API_KEY is None, reason="Missing TMDB_API_KEY")
def test_find_ids():
    ids = tmdb.find_ids(
        ids=["tt0111161", "tt0068646", "tt0468569"],
        source="imdb_id",
        type="movie",
    )
    assert list(ids) == [278, 238, 155]
