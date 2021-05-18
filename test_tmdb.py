import tmdb


def test_movie():
    movie = tmdb.movie("278")
    assert movie
    assert movie["title"] == "The Shawshank Redemption"

    assert not tmdb.movie("1000000000")


def test_find():
    result = tmdb.find(id="tt0111161", source="imdb_id", type="movie")
    assert result
    assert result["id"] == 278
    assert result["title"] == "The Shawshank Redemption"

    result = tmdb.find(id="/m/0479b", source="freebase_mid", type="person")
    assert result
    assert result["id"] == 6384

    assert not tmdb.find(id="tt10000000000", source="imdb_id", type="movie")


def test_external_ids():
    result = tmdb.external_ids(278, type="movie")
    assert result
    assert result["id"] == 278
    assert result["imdb_id"] == "tt0111161"

    result = tmdb.external_ids(1399, type="tv")
    assert result
    assert result["id"] == 1399
    assert result["imdb_id"] == "tt0944947"
    assert result["freebase_mid"] == "/m/0524b41"
    assert result["tvdb_id"] == 121361

    result = tmdb.external_ids(192, type="person")
    assert result
    assert result["id"] == 192
    assert result["imdb_id"] == "nm0000151"
    assert result["freebase_mid"] == "/m/055c8"
