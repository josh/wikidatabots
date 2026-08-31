import os
import platform

import requests

from actions import warn

_API_URL = "https://www.wikidata.org/w/api.php"
_USER_AGENT = f"Josh404Bot/1.0 (User:Josh404Bot) Python/{platform.python_version()}"
_MAXLAG = 5


class MaxlagWarning(Warning):
    pass


def skip_reason(maxlag: int) -> str | None:
    r = requests.get(
        _API_URL,
        params={
            "action": "query",
            "meta": "tokens",
            "type": "login",
            "format": "json",
            "maxlag": str(maxlag),
        },
        headers={"User-Agent": _USER_AGENT},
        timeout=(1, 20),
    )

    if r.status_code == 429:
        return "rate limited by Wikidata (HTTP 429)"
    r.raise_for_status()
    data = r.json()

    if error := data.get("error"):
        if error.get("code") != "maxlag":
            raise ValueError(f"unexpected API error: {error.get('code')}")
        return (
            f"replication lag {float(error['lag']):,.1f}s on {error.get('host', '?')} "
            f"({error.get('type', '?')}), threshold {maxlag}s"
        )

    if not data.get("query", {}).get("tokens", {}).get("logintoken"):
        raise ValueError("unexpected API response")
    return None


def _main() -> None:
    reason = skip_reason(_MAXLAG)
    if reason:
        warn(reason, MaxlagWarning)

    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a") as f:
            f.write(f"ok={'false' if reason else 'true'}\n")


if __name__ == "__main__":
    _main()
