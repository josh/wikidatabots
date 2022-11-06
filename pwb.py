"""
Pywikibot login wrapper.
"""

import os

import pywikibot
import pywikibot.config

SITE = pywikibot.Site("wikidata", "wikidata")

pywikibot.config.password_file = "user-password.py"


def login(username: str, password: str):
    """
    Log into Wikidata.

    Writes an authenticated pywikibot.lwp to the current working directory.
    """
    filename = pywikibot.config.password_file
    assert filename
    with open(filename, "w") as file:
        file.write(f'("{username}", "{password}")')
    os.chmod(filename, 0o600)

    pywikibot.config.usernames["wikidata"]["wikidata"] = username

    SITE.login()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pywikibot wrapper script")
    parser.add_argument("--username", action="store")
    parser.add_argument("--password", action="store")
    parser.add_argument("cmd", action="store")
    args = parser.parse_args()

    if args.cmd == "login":
        login(
            args.username or os.environ["WIKIDATA_USERNAME"],
            args.password or os.environ["WIKIDATA_PASSWORD"],
        )
    else:
        parser.print_usage()
