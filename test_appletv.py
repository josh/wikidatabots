import appletv


def test_movie():
    info = appletv.movie("umc.cmc.459n4f98t82t8ommdoa7ebnny")
    assert info
    assert info["id"] == "umc.cmc.459n4f98t82t8ommdoa7ebnny"
    assert info["itunes_id"] == 282875479


def test_fetch_sitemap_index_urls():
    urls = appletv.fetch_sitemap_index_urls()
    assert len(urls) > 10
    for url in urls:
        assert url.endswith(".xml.gz")
