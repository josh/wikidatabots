import datetime

from tmdb_etl import tmdb_changes


def test_tmdb_changes():
    date = datetime.date(2023, 1, 1)
    df = tmdb_changes(date=date, tmdb_type="movie")
    assert len(df) == 2907
    assert df[0, "date"] == date
    assert df[1, "date"] == date
    assert df[2, "date"] == date
    assert df[0, "id"] == 1068346
    assert df[1, "id"] == 22679
    assert df[2, "id"] == 475946
    assert df[0, "adult"] is None
    assert df[1, "adult"] is False
    assert df[2, "adult"] is False
