import os

import pywikibot.config
from pywikibot import Site

SITE = Site("wikidata", "wikidata")

if "WIKIDATA_USERNAME" in os.environ:
    pywikibot.config.usernames["wikidata"]["wikidata"] = os.environ["WIKIDATA_USERNAME"]

pywikibot.config.password_file = "user-password.py"
