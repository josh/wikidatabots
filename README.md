# Wikidata Bots

[Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) bots running under [Josh404Bot](https://www.wikidata.org/wiki/User:Josh404Bot).

## Usage

```sh
$ go build
$ ./wikidatabots opencritic
$ ./wikidatabots tmdb
```

Each bot prints the edits it wants to make as RDF statements on stdout, for `wikidata-rdf-patch` to apply. Progress and warnings go to stderr. Nothing is printed to stdout unless the whole run succeeds; a failed run exits with status 1.

| Bot | What it does |
| --- | --- |
| `opencritic` | Adds and refreshes OpenCritic review scores. |
| `tmdb` | Adds TMDB IDs found through IMDb and TheTVDB IDs, and deprecates TMDB IDs that no longer exist. |

Output is capped at 250 statements, sampled at random when there are more. `-limit n` changes the cap and `-limit 0` removes it.

The `tmdb` bot needs one of these environment variables:

| Variable | Purpose |
| --- | --- |
| `TMDB_API_KEY` | TMDB API key, sent as the `api_key` query parameter. |
| `TMDB_API_URL` | Base URL to use instead of `https://api.themoviedb.org`, such as a proxy that adds the key itself. |
