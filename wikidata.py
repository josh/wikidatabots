import json
import uuid

import backoff
import requests


def ratelimited(resp):
    for error in resp.get("errors", []):
        if error["code"] == "ratelimited":
            return True

        if error["code"] == "maxlag":
            return True

    return False


@backoff.on_predicate(backoff.constant, ratelimited, interval=5)
def wbsetclaim(claim):
    """
    See <https://www.wikidata.org/w/api.php?action=help&modules=wbsetclaim>.
    """

    data = {
        "action": "wbsetclaim",
        "format": "json",
        "maxlag": "5",
        "errorformat": "plaintext",
        "claim": json.dumps(claim),
        "token": "+\\",
        "bot": "1",
        "ignoreduplicatemainsnak": "1",
    }

    headers = {"User-Agent": "???"}

    r = requests.post("https://www.wikidata.org/w/api.php", data=data, headers=headers)
    r.raise_for_status()
    return r.json()


def new_statement(entity, property, value):
    assert type(entity) is str
    assert type(property) is str
    assert type(value) is str

    return {
        "id": "{}${}".format(entity, uuid.uuid4()),
        "type": "statement",
        "mainsnak": {
            "snaktype": "value",
            "property": property,
            "datavalue": {"value": value, "type": "string"},
        },
        "rank": "normal",
    }
