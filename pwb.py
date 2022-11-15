# pyright: basic

"""
Pywikibot login wrapper.
"""

import os

import pywikibot
import pywikibot.config


def login(username: str, password: str):
    """
    Log into Wikidata.

    Writes an authenticated pywikibot.lwp to the current working directory.
    """

    pywikibot.config.password_file = "user-password.py"
    with open(pywikibot.config.password_file, "w") as file:
        file.write(f'("{username}", "{password}")')
    os.chmod(pywikibot.config.password_file, 0o600)

    pywikibot.config.usernames["wikidata"]["wikidata"] = username

    site = pywikibot.Site("wikidata", "wikidata")
    site.login()


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
