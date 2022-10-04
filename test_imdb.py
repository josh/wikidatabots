import requests

from imdb import ID, canonical_id, extract_id, formatted_url


def test_extract_id():
    assert extract_id("/title/tt0114369/") == "tt0114369"
    assert extract_id("/title/tt0111161/") == "tt0111161"
    assert extract_id("https://www.imdb.com/title/tt0114369/") == "tt0114369"
    assert extract_id("https://www.imdb.com/title/tt0111161") == "tt0111161"
    assert extract_id("https://www.imdb.com/title/tt0111161/") == "tt0111161"
    assert (
        extract_id("https://www.imdb.com/title/tt0111161/?ref_=fn_al_tt_3")
        == "tt0111161"
    )

    assert extract_id("/name/nm0000399/") == "nm0000399"
    assert extract_id("https://www.imdb.com/name/nm0000399/") == "nm0000399"
    assert extract_id("https://www.imdb.com/name/nm0000151") == "nm0000151"
    assert extract_id("https://www.imdb.com/name/nm0000151/") == "nm0000151"
    assert extract_id("/name/nm0000151/") == "nm0000151"
    assert (
        extract_id("https://www.imdb.com/name/nm0000151/?ref_=tt_ov_st") == "nm0000151"
    )

    assert (
        extract_id("https://www.imdb.com/search/title/?companies=co0071326")
        == "co0071326"
    )
    assert (
        extract_id("https://www.imdb.com/search/title/?companies=co0018704")
        == "co0018704"
    )
    assert extract_id("https://www.imdb.com/company/co0018704/") == "co0018704"

    assert extract_id("https://www.imdb.com/character/ch0348376/") == "ch0348376"

    assert extract_id("https://www.imdb.com/event/ev0000292/1997") == "ev0000292/1997"
    assert extract_id("https://www.imdb.com/event/ev0000292/1997/") == "ev0000292/1997"
    assert extract_id("https://www.imdb.com/event/ev0000292/1997/1") == "ev0000292/1997"
    assert extract_id("https://www.imdb.com/event/ev0000203") == "ev0000203"
    assert extract_id("/event/ev0000203/2021/1") == "ev0000203/2021"

    assert not extract_id(
        "https://www.imdb.com/title/tt11696836/characters/nm11012957/"
    )

    assert not extract_id("https://elsewhere.com/title/tt0111161/")


def test_formatted_url():
    assert formatted_url("tt0114369") == "https://www.imdb.com/title/tt0114369/"
    assert formatted_url("tt0111161") == "https://www.imdb.com/title/tt0111161/"

    assert formatted_url("nm0000151") == "https://www.imdb.com/name/nm0000151/"
    assert formatted_url("nm0000399") == "https://www.imdb.com/name/nm0000399/"

    assert (
        formatted_url("co0018704")
        == "https://www.imdb.com/search/title/?companies=co0018704"
    )
    assert (
        formatted_url("co0071326")
        == "https://www.imdb.com/search/title/?companies=co0071326"
    )

    assert (
        formatted_url("ev0000764/1967/1")
        == "https://www.imdb.com/event/ev0000764/1967/1"
    )
    assert (
        formatted_url("ev0000292/1997") == "https://www.imdb.com/event/ev0000292/1997/1"
    )
    assert formatted_url("ev0000203") == "https://www.imdb.com/event/ev0000203"

    assert formatted_url("ch0348376") == "https://www.imdb.com/character/ch0348376/"

    assert not formatted_url("tt11696836/characters/nm11012957")
    assert not formatted_url("0000399")
    assert not formatted_url("junk")
    assert not formatted_url("/title/tt0111161/")


def externalid_url(id: ID) -> str:
    url = (
        "https://wikidata-externalid-url.toolforge.org/?"
        + "p=345&"
        + "url_prefix=https://www.imdb.com/&id={}".format(id)
    )
    r = requests.get(url)
    return r.url


def test_externalid_url():
    assert externalid_url("tt0068646") == formatted_url("tt0068646")
    assert externalid_url("nm1827914") == formatted_url("nm1827914")
    assert externalid_url("co0018704") == formatted_url("co0018704")
    assert externalid_url("ev0000292/1997") == formatted_url("ev0000292/1997")


def test_canonical_id():
    assert canonical_id("tt0111161") == "tt0111161"
    assert canonical_id("nm0000151") == "nm0000151"
    assert canonical_id("co0018704") == "co0018704"
    assert canonical_id("ev0000292/1997") == "ev0000292/1997"
    assert canonical_id("ev0000203") == "ev0000203"

    assert canonical_id("ch0348376") == "ch0348376"

    assert canonical_id("tt11639970") == "tt2177268"

    assert not canonical_id("tt100000000")
    assert not canonical_id("tt1555101")
