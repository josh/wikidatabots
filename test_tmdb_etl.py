import datetime

import pandas as pd

from tmdb_etl import tmdb_changes


def test_tmdb_changes():
    date = datetime.date(2023, 1, 1)
    df = tmdb_changes(date=date, tmdb_type="movie")
    assert len(df) == 2907
    assert df.at[0, "date"] == date
    assert df.at[1, "date"] == date
    assert df.at[2, "date"] == date
    assert df.at[0, "id"] == 1068346
    assert df.at[1, "id"] == 22679
    assert df.at[2, "id"] == 475946
    assert pd.isna(df.at[0, "adult"])
    assert not df.at[1, "adult"]
    assert not df.at[2, "adult"]
