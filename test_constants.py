import pywikibot

import constants


def test_pids():
    site = pywikibot.Site("wikidata", "wikidata")

    for (name, value) in constants.__dict__.items():
        if name.endswith("_PID"):
            page = pywikibot.PropertyPage(site, value)
            assert page.get()
            assert page.exists()
            label = page.labels["en"]
            assert label and type(label) is str
            assert name == label.upper().replace(" ", "_").replace("/", "_") + "_PID"


def test_qids():
    site = pywikibot.Site("wikidata", "wikidata")

    for (name, value) in constants.__dict__.items():
        if name.endswith("_QID"):
            page = pywikibot.ItemPage(site, value)
            assert page.get()
            assert page.exists()
            label = page.labels["en"]
            assert label and type(label) is str
            assert name == label.upper().replace(" ", "_").replace("/", "_") + "_QID"
