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
