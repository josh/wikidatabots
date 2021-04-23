import tmdb


def test_movie():
    movie = tmdb.movie("278")
    assert movie
    assert movie["title"] == "The Shawshank Redemption"


def test_find():
    results = tmdb.find("tt0111161", "imdb_id")
    assert results
    assert results["movie_results"]
    assert results["movie_results"][0]["title"] == "The Shawshank Redemption"


def test_find_by_imdb_id():
    tmdb_id = tmdb.find_by_imdb_id("tt0111161", type="movie")
    assert tmdb_id == "278"
