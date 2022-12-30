# pyright: basic

from . import cleaned_sitemap, siteindex, sitemap


def test_siteindex():
    # siteindex(type="episode")
    # siteindex(type="movie")
    siteindex(type="show")


def test_sitemap():
    # sitemap(type="episode")
    # sitemap(type="movie")
    sitemap(type="show")


def test_cleaned_sitemap():
    # cleaned_sitemap(type="episode")
    # cleaned_sitemap(type="movie")
    cleaned_sitemap(type="show")
