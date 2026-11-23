package main

import (
	"context"
	"fmt"
	"slices"
	"testing"
)

func TestIMDbNumericID(t *testing.T) {
	tests := []struct {
		kind tmdbType
		id   string
		want uint32
		ok   bool
	}{
		{tmdbMovie, "tt0111161", 111161, true},
		{tmdbTV, "tt0944947", 944947, true},
		{tmdbPerson, "nm0000151", 151, true},
		{tmdbPerson, "tt0111161", 0, false},
		{tmdbMovie, "nm0000151", 0, false},
		{tmdbMovie, "", 0, false},
		{tmdbMovie, "tt99999999999", 0, false},
	}
	for _, tt := range tests {
		got, ok, err := imdbNumericID(tt.kind.imdbPattern, tt.id)
		if err != nil || got != tt.want || ok != tt.ok {
			t.Errorf("imdbNumericID(%s, %q) = %d, %v, %v", tt.kind.name, tt.id, got, ok, err)
		}
	}
}

func TestTMDBCandidates(t *testing.T) {
	item := func(n int) string { return fmt.Sprintf("http://www.wikidata.org/entity/Q%d", n) }
	rows := []externalIDRow{
		{item: item(1), externalID: "tt0000001"},
		{item: item(2), externalID: "tt0000002", hasTMDBID: true},
		{item: item(3), externalID: "tt0000003"}, // shares an ID with Q4
		{item: item(4), externalID: "tt0000003"},
		{item: item(5), externalID: "tt0000005"}, // blocked
		{item: item(6), externalID: "tt0000006"}, // unknown to the index
		{item: item(7), externalID: "nm0000007"}, // not a title ID
		{item: item(8), externalID: "tt0000008"}, // shares an ID with Q9, which already has a TMDB ID
		{item: item(9), externalID: "tt0000008", hasTMDBID: true},
		{item: item(10), externalID: "tt0000010"},
	}
	known := map[uint32]struct{}{1: {}, 2: {}, 3: {}, 5: {}, 7: {}, 8: {}, 10: {}}
	blocked := blocklist{"Q5": {}}

	candidates, err := tmdbCandidates(rows, known, blocked, func(id string) (uint32, bool, error) {
		return imdbNumericID(titlePattern, id)
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range candidates {
		got = append(got, c.externalID)
	}
	if want := []string{"tt0000001", "tt0000010"}; !slices.Equal(got, want) {
		t.Errorf("candidates %q, want %q", got, want)
	}
}

func TestAddStatementsRequestCap(t *testing.T) {
	client, err := newTMDBClient("", "KEY")
	if err != nil {
		t.Fatal(err)
	}
	tooMany := make([]externalIDRow, tmdbMaxRequests)
	if _, err := addStatements(context.Background(), client, tmdbMovie, tooMany, "imdb_id", ""); err == nil {
		t.Error("request cap should be an error")
	}
}

func TestNewTMDBClient(t *testing.T) {
	if _, err := newTMDBClient("", ""); err == nil {
		t.Error("no URL and no key should be an error")
	}
	client, err := newTMDBClient("", "KEY")
	if err != nil || client.baseURL != "https://api.themoviedb.org" {
		t.Errorf("default base URL: %+v, %v", client, err)
	}
	client, err = newTMDBClient("http://proxy.example/tmdb/", "")
	if err != nil || client.baseURL != "http://proxy.example/tmdb" || client.apiKey != "" {
		t.Errorf("proxy base URL: %+v, %v", client, err)
	}
}
