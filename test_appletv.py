# pyright: strict

import appletv


def test_movie():
    info = appletv.movie(appletv.id("umc.cmc.459n4f98t82t8ommdoa7ebnny"))
    assert info
    assert info["id"] == "umc.cmc.459n4f98t82t8ommdoa7ebnny"
    assert info["itunes_id"] == 282875479


def test_fetch_sitemap_index_urls():
    urls = list(appletv.fetch_sitemap_index_urls())
    assert len(urls) > 10
    for url in urls:
        assert url.endswith(".xml.gz")


def test_fetch_new_sitemap_urls():
    urls = list(appletv.fetch_new_sitemap_urls())
    for url in urls:
        assert url.startswith("https://tv.apple.com")
