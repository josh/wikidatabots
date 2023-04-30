# pyright: reportGeneralTypeIssues=false

import os

import pywikibot
import pywikibot.config


def login(username: str, password: str) -> None:
    pywikibot.config.password_file = "user-password.py"
    with open(pywikibot.config.password_file, "w") as file:
        file.write(f'("{username}", "{password}")')
    os.chmod(pywikibot.config.password_file, 0o600)

    pywikibot.config.usernames["wikidata"]["wikidata"] = username

    site = pywikibot.Site("wikidata", "wikidata")
    site.login()
