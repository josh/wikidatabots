import datetime

from tmdb_etl import tmdb_changes


def test_tmdb_changes():
    date = datetime.date(2023, 1, 1)
    tmdb_changes(date=date, tmdb_type="movie")
