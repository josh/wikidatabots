package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	tmdbDefaultBaseURL = "https://api.themoviedb.org"
	tmdbIndexURL       = "https://josh.github.io/tmdb-index/tmdb-%s.parquet"

	// A guard against a bad query or index turning into a flood of requests.
	tmdbMaxRequests = 50_000
)

type tmdbType struct {
	name        string // TMDB API path segment and index file suffix
	property    string // Wikidata property holding the TMDB ID
	label       string
	imdbPattern *regexp.Regexp
	imdbQueries []string
}

var (
	titlePattern = regexp.MustCompile(`tt(\d+)`)
	namePattern  = regexp.MustCompile(`nm(\d+)`)

	tmdbMovie = tmdbType{
		name:        "movie",
		property:    "P4947",
		label:       "TMDb movie ID",
		imdbPattern: titlePattern,
		imdbQueries: []string{
			imdbQuery("P4947", "?item (wdt:P31/(wdt:P279*)) wd:Q11424."),   // film
			imdbQuery("P4947", "?item (wdt:P31/(wdt:P279*)) wd:Q24856."),   // film series
			imdbQuery("P4947", "?item (wdt:P31/(wdt:P279*)) wd:Q1261214."), // television special
		},
	}
	tmdbTV = tmdbType{
		name:        "tv",
		property:    "P4983",
		label:       "TMDb TV series ID",
		imdbPattern: titlePattern,
		imdbQueries: []string{
			imdbQuery("P4983", "?item (wdt:P31/(wdt:P279*)) wd:Q15416."),   // television program
			imdbQuery("P4983", "?item (wdt:P31/(wdt:P279*)) wd:Q5398426."), // television series
		},
	}
	tmdbPerson = tmdbType{
		name:        "person",
		property:    "P4985",
		label:       "TMDb person ID",
		imdbPattern: namePattern,
		imdbQueries: []string{
			imdbQuery("P4985", "?item wdt:P31 wd:Q5."),        // human
			imdbQuery("P4985", "?item wdt:P31 wd:Q16334295."), // group of humans
		},
	}
)

func imdbQuery(property, class string) string {
	return fmt.Sprintf(`
SELECT DISTINCT ?item ?imdb_id ?tmdb_id WHERE {
  ?item wdt:P345 ?imdb_id.

  %s

  OPTIONAL {
    ?item wdt:%s ?tmdb_id.
    FILTER(xsd:integer(?tmdb_id))
  }
}
`, class, property)
}

const tvdbQuery = `
SELECT DISTINCT ?item ?tvdb_id ?tmdb_id WHERE {
  ?item wdt:P4835 ?tvdb_id.

  # TMDb TV series ID subject type constraints
  VALUES ?class {
    wd:Q15416 # television program
    wd:Q5398426 # television series
  }
  ?item (wdt:P31/(wdt:P279*)) ?class.

  FILTER(xsd:integer(?tvdb_id))

  OPTIONAL {
    ?item wdt:P4983 ?tmdb_id.
    FILTER(xsd:integer(?tmdb_id))
  }
}
`

const notDeprecatedQuery = `
SELECT ?statement ?id WHERE {
  ?statement ps:%s ?id.
  ?statement wikibase:rank ?rank.
  FILTER(?rank != wikibase:DeprecatedRank)
  FILTER(xsd:integer(?id))
}
`

func tmdbBot(ctx context.Context) ([]string, error) {
	client, err := newTMDBClient(os.Getenv("TMDB_API_URL"), os.Getenv("TMDB_API_KEY"))
	if err != nil {
		return nil, err
	}
	blocked, err := fetchBlocklist(ctx)
	if err != nil {
		return nil, err
	}

	indexes := map[string]*tmdbIndex{}
	for _, t := range []tmdbType{tmdbMovie, tmdbTV, tmdbPerson} {
		index, err := loadTMDBIndex(ctx, t)
		if err != nil {
			return nil, err
		}
		indexes[t.name] = index
	}

	jobs := []func() ([]string, error){
		func() ([]string, error) { return addViaIMDb(ctx, client, tmdbMovie, indexes["movie"], blocked) },
		func() ([]string, error) { return addViaIMDb(ctx, client, tmdbTV, indexes["tv"], blocked) },
		func() ([]string, error) { return addViaTVDB(ctx, client, tmdbTV, indexes["tv"], blocked) },
		func() ([]string, error) { return addViaIMDb(ctx, client, tmdbPerson, indexes["person"], blocked) },
		func() ([]string, error) { return deprecateNotFound(ctx, client, tmdbMovie, indexes["movie"]) },
		func() ([]string, error) { return deprecateNotFound(ctx, client, tmdbTV, indexes["tv"]) },
		func() ([]string, error) { return deprecateNotFound(ctx, client, tmdbPerson, indexes["person"]) },
	}
	var statements []string
	for _, job := range jobs {
		s, err := job()
		if err != nil {
			return nil, err
		}
		statements = append(statements, s...)
	}
	return statements, nil
}

// tmdbIndex is the part of a tmdb-index snapshot the bot needs: which external
// IDs TMDB knows about, and which TMDB IDs failed to load when last crawled.
type tmdbIndex struct {
	imdbIDs map[uint32]struct{}
	tvdbIDs map[uint32]struct{}
	failed  map[uint32]struct{}
}

func loadTMDBIndex(ctx context.Context, t tmdbType) (*tmdbIndex, error) {
	file, err := downloadParquet(ctx, fmt.Sprintf(tmdbIndexURL, t.name))
	if err != nil {
		return nil, err
	}
	index := &tmdbIndex{
		imdbIDs: map[uint32]struct{}{},
		tvdbIDs: map[uint32]struct{}{},
		failed:  map[uint32]struct{}{},
	}
	columns := []column{
		{"id", parquet.Int32},
		{"success", parquet.Boolean},
		{"imdb_numeric_id", parquet.Int32},
	}
	if t.name == "tv" {
		columns = append(columns, column{"tvdb_id", parquet.Int32})
	}
	err = scanColumns(file, columns, func(row []parquet.Value) error {
		if row[0].IsNull() {
			return nil
		}
		if !row[1].IsNull() && !row[1].Boolean() {
			index.failed[uint32(row[0].Int32())] = struct{}{}
		}
		if !row[2].IsNull() {
			index.imdbIDs[uint32(row[2].Int32())] = struct{}{}
		}
		if len(row) > 3 && !row[3].IsNull() {
			index.tvdbIDs[uint32(row[3].Int32())] = struct{}{}
		}
		return nil
	})
	return index, err
}

// externalIDRow is a Wikidata item with an external ID and, if it already has
// one, a TMDB ID.
type externalIDRow struct {
	item       string
	externalID string
	hasTMDBID  bool
}

func addViaIMDb(ctx context.Context, client *tmdbClient, t tmdbType, index *tmdbIndex, blocked blocklist) ([]string, error) {
	var rows []externalIDRow
	for _, query := range t.imdbQueries {
		result, err := sparqlQuery(ctx, query, "item", "imdb_id", "tmdb_id")
		if err != nil {
			return nil, err
		}
		for _, r := range result {
			rows = append(rows, externalIDRow{item: r[0], externalID: r[1], hasTMDBID: r[2] != ""})
		}
	}
	candidates, err := tmdbCandidates(rows, index.imdbIDs, blocked, func(imdbID string) (uint32, bool, error) {
		return imdbNumericID(t.imdbPattern, imdbID)
	})
	if err != nil {
		return nil, err
	}
	summary := fmt.Sprintf("Add %s claim via associated IMDb ID", t.label)
	return addStatements(ctx, client, t, candidates, "imdb_id", summary)
}

func addViaTVDB(ctx context.Context, client *tmdbClient, t tmdbType, index *tmdbIndex, blocked blocklist) ([]string, error) {
	result, err := sparqlQuery(ctx, tvdbQuery, "item", "tvdb_id", "tmdb_id")
	if err != nil {
		return nil, err
	}
	rows := make([]externalIDRow, len(result))
	for i, r := range result {
		rows[i] = externalIDRow{item: r[0], externalID: r[1], hasTMDBID: r[2] != ""}
	}
	candidates, err := tmdbCandidates(rows, index.tvdbIDs, blocked, func(tvdbID string) (uint32, bool, error) {
		if tvdbID == "" {
			return 0, false, nil
		}
		id, err := strconv.ParseUint(tvdbID, 10, 32)
		if err != nil {
			return 0, false, fmt.Errorf("TheTVDB ID: %w", err)
		}
		return uint32(id), true, nil
	})
	if err != nil {
		return nil, err
	}
	// The numeric form drops leading zeros, which TMDB would not match.
	for i := range candidates {
		id, _ := strconv.ParseUint(candidates[i].externalID, 10, 32)
		candidates[i].externalID = strconv.FormatUint(id, 10)
	}
	summary := fmt.Sprintf("Add %s claim via associated TheTVDB.com series ID", t.label)
	return addStatements(ctx, client, t, candidates, "tvdb_id", summary)
}

// imdbNumericID extracts the number from an IMDb ID such as tt0111161. IDs
// that do not match, or do not fit in 32 bits, have no numeric form.
func imdbNumericID(pattern *regexp.Regexp, imdbID string) (uint32, bool, error) {
	match := pattern.FindStringSubmatch(imdbID)
	if match == nil {
		return 0, false, nil
	}
	id, err := strconv.ParseUint(match[1], 10, 32)
	if err != nil {
		return 0, false, nil
	}
	return uint32(id), true, nil
}

// tmdbCandidates picks the items worth asking TMDB about: no TMDB ID yet, not
// blocked, an external ID the index knows, and that external ID not shared
// with any other row. A shared ID means Wikidata has duplicate or conflicting
// items, so none of them is edited.
func tmdbCandidates(rows []externalIDRow, known map[uint32]struct{}, blocked blocklist, numeric func(string) (uint32, bool, error)) ([]externalIDRow, error) {
	ids := make([]uint32, len(rows))
	valid := make([]bool, len(rows))
	counts := make(map[uint32]int, len(rows))
	for i, row := range rows {
		id, ok, err := numeric(row.externalID)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", row.item, err)
		}
		ids[i], valid[i] = id, ok
		if ok {
			counts[id]++
		}
	}

	var candidates []externalIDRow
	for i, row := range rows {
		if !valid[i] || counts[ids[i]] != 1 || row.hasTMDBID || row.item == "" || !blocked.allows(row.item) {
			continue
		}
		if _, ok := known[ids[i]]; ok {
			candidates = append(candidates, row)
		}
	}
	return candidates, nil
}

func addStatements(ctx context.Context, client *tmdbClient, t tmdbType, candidates []externalIDRow, source, summary string) ([]string, error) {
	if len(candidates) >= tmdbMaxRequests {
		return nil, fmt.Errorf("tmdb find %s via %s: too many requests: %d", t.name, source, len(candidates))
	}
	progress := newProgress(fmt.Sprintf("tmdb find %s via %s", t.name, source), len(candidates))
	var statements []string
	for _, candidate := range candidates {
		id, found, err := client.find(ctx, t, source, candidate.externalID)
		if err != nil {
			return nil, err
		}
		if found {
			statements = append(statements, fmt.Sprintf(`<%s> wdt:%s "%d" ; wikidatabots:editSummary "%s" .`, candidate.item, t.property, id, summary))
		}
		progress.step()
	}
	return statements, nil
}

// deprecateNotFound deprecates TMDB ID statements whose ID the index failed to
// load and TMDB itself confirms is gone.
func deprecateNotFound(ctx context.Context, client *tmdbClient, t tmdbType, index *tmdbIndex) ([]string, error) {
	rows, err := sparqlQuery(ctx, fmt.Sprintf(notDeprecatedQuery, t.property), "statement", "id")
	if err != nil {
		return nil, err
	}

	type suspect struct {
		statement string
		id        uint32
	}
	var suspects []suspect
	for _, row := range rows {
		if row[1] == "" {
			continue
		}
		id, err := strconv.ParseUint(row[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", row[0], t.label, err)
		}
		if _, failed := index.failed[uint32(id)]; failed {
			suspects = append(suspects, suspect{row[0], uint32(id)})
		}
	}
	if len(suspects) >= tmdbMaxRequests {
		return nil, fmt.Errorf("tmdb %s: too many requests: %d", t.name, len(suspects))
	}

	// A movie property value may also be a collection ID.
	kinds := []string{t.name}
	if t.name == "movie" {
		kinds = append(kinds, "collection")
	}

	progress := newProgress("tmdb "+t.name, len(suspects))
	var statements []string
	for _, s := range suspects {
		exists := false
		for _, kind := range kinds {
			exists, err = client.exists(ctx, kind, s.id)
			if err != nil {
				return nil, err
			}
			if exists {
				break
			}
		}
		if !exists {
			statements = append(statements, fmt.Sprintf(`<%s> wikibase:rank wikibase:DeprecatedRank ; pq:P2241 wd:Q21441764 ; wikidatabots:editSummary "Deprecate removed TMDB %s ID" .`, s.statement, t.name))
		}
		progress.step()
	}
	return statements, nil
}

type tmdbClient struct {
	baseURL string
	apiKey  string
	backoff backoff
}

// newTMDBClient needs a key, a base URL, or both. A base URL without a key is
// for a proxy that adds the key itself.
func newTMDBClient(baseURL, apiKey string) (*tmdbClient, error) {
	if baseURL == "" && apiKey == "" {
		return nil, errors.New("TMDB_API_URL or TMDB_API_KEY must be set")
	}
	if baseURL == "" {
		baseURL = tmdbDefaultBaseURL
	}
	return &tmdbClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		backoff: backoff{maxTries: 3, base: time.Second, maxTime: 300 * time.Second},
	}, nil
}

// get returns the body of a 200 or 404 response; TMDB answers 404 with a JSON
// error object for IDs it does not have.
func (c *tmdbClient) get(ctx context.Context, path string, params ...string) ([]byte, error) {
	if c.apiKey != "" {
		params = append(params, "api_key="+url.QueryEscape(c.apiKey))
	}
	rawURL := c.baseURL + path
	if len(params) > 0 {
		rawURL += "?" + strings.Join(params, "&")
	}
	return retry(ctx, "tmdb", c.backoff, func() ([]byte, error) {
		_, body, err := get(ctx, apiClient, rawURL, http.StatusOK, http.StatusNotFound)
		return body, err
	})
}

// find looks up the TMDB ID of the given type for an external ID.
func (c *tmdbClient) find(ctx context.Context, t tmdbType, source, externalID string) (uint32, bool, error) {
	path := "/3/find/" + url.PathEscape(externalID)
	body, err := c.get(ctx, path, "external_source="+source)
	if err != nil {
		return 0, false, err
	}
	var response map[string]json.RawMessage
	if err := json.Unmarshal(body, &response); err != nil {
		return 0, false, fmt.Errorf("tmdb: GET %s: %w", path, err)
	}
	var results []struct {
		ID *uint32 `json:"id"`
	}
	if raw, ok := response[t.name+"_results"]; ok {
		if err := json.Unmarshal(raw, &results); err != nil {
			return 0, false, fmt.Errorf("tmdb: GET %s: %w", path, err)
		}
	}
	if len(results) == 0 || results[0].ID == nil {
		return 0, false, nil
	}
	return *results[0].ID, true, nil
}

// exists reports whether TMDB has an object of the given kind and ID.
func (c *tmdbClient) exists(ctx context.Context, kind string, id uint32) (bool, error) {
	path := fmt.Sprintf("/3/%s/%d", kind, id)
	body, err := c.get(ctx, path)
	if err != nil {
		return false, err
	}
	var response struct {
		ID *uint32 `json:"id"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return false, fmt.Errorf("tmdb: GET %s: %w", path, err)
	}
	return response.ID != nil, nil
}

// progress logs how far a long run of requests has got.
type progress struct {
	desc        string
	done, total int
	last        time.Time
}

func newProgress(desc string, total int) *progress {
	log.Printf("%s: %d requests", desc, total)
	return &progress{desc: desc, total: total, last: time.Now()}
}

func (p *progress) step() {
	p.done++
	if p.done < p.total && time.Since(p.last) >= 30*time.Second {
		log.Printf("%s: %d/%d", p.desc, p.done, p.total)
		p.last = time.Now()
	}
}
