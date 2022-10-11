from pywikibot import ItemPage

import items


def test_items_exist():
    for (name, page) in items.__dict__.items():
        if name.endswith("_ITEM"):
            assert isinstance(page, ItemPage)
            assert page.get()
            assert page.exists()
