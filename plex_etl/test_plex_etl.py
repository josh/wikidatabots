# pyright: basic

from .plex_etl import wd_plex_guids


def test_wd_plex_guids():
    df = wd_plex_guids()
    assert len(df) > 0
    assert df.dtypes["guid"] == "string"
    assert df.dtypes["type"] == "category"
    assert df.dtypes["key"] == "binary[pyarrow]"
    assert df["guid"].is_unique
    assert df["key"].is_unique
