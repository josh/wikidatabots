from pywikibot import PropertyPage

import properties


def test_items_exist():
    for (name, page) in properties.__dict__.items():
        if name.endswith("_PROPERTY"):
            assert isinstance(page, PropertyPage)
            assert page.get()
            assert page.exists()
