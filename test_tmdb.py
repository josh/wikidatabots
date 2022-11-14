# pyright: strict

import datetime

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
def test_changes():
    ids = tmdb.changes(type="movie")
    assert len(list(ids)) > 0

    start_date = datetime.date.today() - datetime.timedelta(days=3)
    ids = tmdb.changes(type="movie", start_date=start_date)
    assert len(list(ids)) > 0

    ids = tmdb.changes(type="tv")
    assert len(list(ids)) > 0

    ids = tmdb.changes(type="person")
    assert len(list(ids)) > 0
