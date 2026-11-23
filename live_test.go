package main

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
)

// These tests talk to the real services. Skip them with `go test -short`.

func TestSPARQLQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("network")
	}
	rows, err := sparqlQuery(context.Background(), `
SELECT ?item ?missing ?imdb_id WHERE {
  ?item wdt:P345 ?imdb_id.
  OPTIONAL { ?item wdt:P0 ?missing. }
}
LIMIT 10
`, "imdb_id", "item", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 {
		t.Fatalf("got %d rows, want 10", len(rows))
	}
	for _, row := range rows {
		if row[0] == "" || !strings.HasPrefix(row[1], "http://www.wikidata.org/entity/Q") || row[2] != "" {
			t.Errorf("unexpected row %q", row)
		}
	}

	if _, err := sparqlQuery(context.Background(), "SELECT ?item WHERE { ?item wdt:P345 ?x } LIMIT 1", "nope"); err == nil {
		t.Error("missing column should be an error")
	}
}

func TestFetchBlocklist(t *testing.T) {
	if testing.Short() {
		t.Skip("network")
	}
	blocked, err := fetchBlocklist(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d blocked items", len(blocked))
	for qid := range blocked {
		if blocked.allows("http://www.wikidata.org/entity/" + qid) {
			t.Errorf("%s should be blocked", qid)
		}
	}
}

func TestLoadOpencriticGames(t *testing.T) {
	if testing.Short() {
		t.Skip("network")
	}
	games, err := loadOpencriticGames(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// The Witcher 3: Wild Hunt
	game, ok := games[463]
	if !ok || game.topCriticScore == nil || game.numReviews == nil || game.latestReviewDate == nil || game.retrievedOn == nil {
		t.Fatalf("game 463 incomplete: %+v", game)
	}
	if *game.topCriticScore < 80 || *game.numReviews < 100 {
		t.Errorf("game 463: score %v from %d reviews", *game.topCriticScore, *game.numReviews)
	}
	if date := formatDate(*game.retrievedOn); date < "2024-01-01" || date > "2100-01-01" {
		t.Errorf("game 463 retrieved on %s", date)
	}
}

func TestLoadTMDBIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("network")
	}
	index, err := loadTMDBIndex(context.Background(), tmdbTV)
	if err != nil {
		t.Fatal(err)
	}
	// Game of Thrones: IMDb tt0944947, TheTVDB 121361
	if _, ok := index.imdbIDs[944947]; !ok {
		t.Error("IMDb ID 944947 missing from tv index")
	}
	if _, ok := index.tvdbIDs[121361]; !ok {
		t.Error("TheTVDB ID 121361 missing from tv index")
	}
	if len(index.failed) == 0 {
		t.Error("tv index has no failed IDs")
	}
}

func liveTMDBClient(t *testing.T) *tmdbClient {
	if testing.Short() {
		t.Skip("network")
	}
	client, err := newTMDBClient(os.Getenv("TMDB_API_URL"), os.Getenv("TMDB_API_KEY"))
	if err != nil {
		t.Skip(err)
	}
	return client
}

func TestTMDBFindLive(t *testing.T) {
	client := liveTMDBClient(t)
	ctx := context.Background()
	tests := []struct {
		kind       tmdbType
		source, id string
		want       uint32
		found      bool
	}{
		{tmdbMovie, "imdb_id", "tt1630029", 76600, true},
		{tmdbPerson, "imdb_id", "nm3718007", 1674162, true},
		{tmdbTV, "imdb_id", "tt1630029", 0, false},
	}
	for _, tt := range tests {
		got, found, err := client.find(ctx, tt.kind, tt.source, tt.id)
		if err != nil || got != tt.want || found != tt.found {
			t.Errorf("find %s %s = %d, %v, %v", tt.kind.name, tt.id, got, found, err)
		}
	}
}

func TestTMDBExistsLive(t *testing.T) {
	client := liveTMDBClient(t)
	ctx := context.Background()
	for _, tt := range []struct {
		kind string
		id   uint32
		want bool
	}{
		{"movie", 0, false},
		{"movie", 2, true},
		{"movie", 3, true},
		{"movie", 4, false},
		{"movie", 3106, false},
		{"collection", 2, false},
		{"collection", 87255, true},
	} {
		got, err := client.exists(ctx, tt.kind, tt.id)
		if err != nil || got != tt.want {
			t.Errorf("exists %s %d = %v, %v", tt.kind, tt.id, got, err)
		}
	}
}

func TestAddStatementsLive(t *testing.T) {
	client := liveTMDBClient(t)
	candidates := []externalIDRow{
		{item: "http://www.wikidata.org/entity/Q1", externalID: "tt1630029"},
		{item: "http://www.wikidata.org/entity/Q2", externalID: "tt0000000"},
	}
	got, err := addStatements(context.Background(), client, tmdbMovie, candidates, "imdb_id", "Add TMDb movie ID claim via associated IMDb ID")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`<http://www.wikidata.org/entity/Q1> wdt:P4947 "76600" ; wikidatabots:editSummary "Add TMDb movie ID claim via associated IMDb ID" .`}
	if !slices.Equal(got, want) {
		t.Errorf("got %q", got)
	}
}
