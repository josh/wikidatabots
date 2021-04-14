import requests

import imdb_id


def test_formatted_url():
    assert imdb_id.formatted_url("tt0114369") == "https://www.imdb.com/title/tt0114369/"
    assert imdb_id.formatted_url("nm0000399") == "https://www.imdb.com/name/nm0000399/"
    assert not imdb_id.formatted_url("ch0000985")
    assert not imdb_id.formatted_url("co0071326")
    assert not imdb_id.formatted_url("tt11696836/characters/nm11012957")
    assert not imdb_id.formatted_url("0000399")


def externalid_url(id):
    url = (
        "https://wikidata-externalid-url.toolforge.org/?"
        + "p=345&"
        + "url_prefix=https://www.imdb.com/&id={}".format(id)
    )
    r = requests.head(url)
    return r.headers["Location"]


def test_externalid_url():
    assert externalid_url("tt0068646") == imdb_id.formatted_url("tt0068646")
    assert externalid_url("nm1827914") == imdb_id.formatted_url("nm1827914")


def test_extract_id():
    assert imdb_id.extract_id("/title/tt0114369/") == "tt0114369"
    assert imdb_id.extract_id("https://www.imdb.com/title/tt0114369/") == "tt0114369"
    assert imdb_id.extract_id("/name/nm0000399/") == "nm0000399"
    assert imdb_id.extract_id("https://www.imdb.com/name/nm0000399/") == "nm0000399"
    assert not imdb_id.extract_id(
        "https://www.imdb.com/title/tt11696836/characters/nm11012957/"
    )
    assert not imdb_id.extract_id(
        "https://www.imdb.com/search/title/?companies=co0071326"
    )
