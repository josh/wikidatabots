# pyright: basic

from . import siteindex, sitemap


def test_siteindex():
    # siteindex(type="episode")
    # siteindex(type="movie")
    siteindex(type="show")


def test_sitemap():
    # sitemap(type="episode")
    # sitemap(type="movie")
    sitemap(type="show")
