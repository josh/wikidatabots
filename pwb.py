import os
import tempfile

import pywikibot


def login(username, password):
    password_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    password_file.write('("{}", "{}")'.format(username, password))
    password_file.close()

    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    pywikibot.config.password_file = password_file.name

    site = pywikibot.Site("wikidata", "wikidata")
    site.login()

    os.unlink(password_file.name)


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
