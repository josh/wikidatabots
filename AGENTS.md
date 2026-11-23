# Agents Guide

The bots are written in Go 1.27 and build into a single `wikidatabots` binary.

## Testing

```sh
$ go build
$ gofmt -l .
$ go vet ./...
$ go test ./...
```

Avoid mocks and local fake servers. Tests make real network connections to Wikidata, the index files on josh.github.io, postman-echo.com and TMDB; `go test -short` skips them. The TMDB tests run only when `TMDB_API_URL` or `TMDB_API_KEY` is set.

## Dependencies

The only third-party dependency is `github.com/parquet-go/parquet-go`. Prefer the standard library over adding another.

## Output

stdout is a contract with `wikidata-rdf-patch`: statement text must not change without a matching change there. stderr is free-form.

## Comments

Avoid superfluous comments. Only include them when they add value or clarify complex logic.
