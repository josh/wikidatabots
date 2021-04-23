"""
Wikidata page modifications wrapper using pywikibot.

MUST be logged in first. See pwb.py
"""

import pywikibot


def edit(title, text, username, summary=None):
    """
    Edit an existing wiki page.
    """

    pywikibot.config.usernames["wikidata"]["wikidata"] = username
    site = pywikibot.Site("wikidata", "wikidata")
    page = pywikibot.Page(site, title)
    page.text = text
    page.save(summary)
    return page


if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Create and edit Wikidata pages.")
    parser.add_argument("--username", action="store")
    parser.add_argument("--title", action="store")
    parser.add_argument("--summary", action="store")
    parser.add_argument("cmd", action="store")
    args = parser.parse_args()

    if args.cmd == "edit":
        edit(
            username=args.username or os.environ["WIKIDATA_USERNAME"],
            title=args.title,
            text=sys.stdin.read(),
            summary=args.summary,
        )
    else:
        parser.print_usage()
