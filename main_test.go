package main

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestWriteStatements(t *testing.T) {
	var all []string
	for i := range 20 {
		all = append(all, fmt.Sprintf("<s%d> <p> <o> .", i))
	}

	for _, limit := range []int{0, 20, 21} {
		var out bytes.Buffer
		if err := writeStatements(&out, all, limit); err != nil {
			t.Fatal(err)
		}
		if out.String() != strings.Join(all, "\n")+"\n" {
			t.Errorf("limit %d: output differs from input", limit)
		}
	}

	var out bytes.Buffer
	if err := writeStatements(&out, all, 5); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	slices.Sort(lines)
	if len(lines) != 5 || len(slices.Compact(lines)) != 5 {
		t.Fatalf("limit 5: got %q", lines)
	}
	for _, line := range lines {
		if !slices.Contains(all, line) {
			t.Errorf("limit 5: unexpected line %q", line)
		}
	}

	out.Reset()
	if err := writeStatements(&out, nil, 5); err != nil || out.Len() != 0 {
		t.Errorf("no statements: %q, %v", out.String(), err)
	}
}

func TestRunUsage(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"nope"},
		{"tmdb", "extra"},
		{"-limit", "-1", "tmdb"},
		{"-limit", "x", "tmdb"},
		{"-bogus", "tmdb"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(context.Background(), args, &stdout, &stderr); code != 2 {
			t.Errorf("%q: exit %d, want 2", args, code)
		}
		if stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage: wikidatabots") {
			t.Errorf("%q: stdout %q, stderr %q", args, stdout.String(), stderr.String())
		}
	}
}

func TestRunFailureLeavesStdoutEmpty(t *testing.T) {
	t.Setenv("TMDB_API_URL", "")
	t.Setenv("TMDB_API_KEY", "")
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"tmdb"}, &stdout, &stderr); code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "TMDB_API_URL or TMDB_API_KEY must be set") {
		t.Errorf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
}

func TestParseBlocklist(t *testing.T) {
	blocked, err := parseBlocklist([]byte(`{"query":{"pages":{"103442925":{"extract":"Q42\nSee also Q1000 and Q7."}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(blocked) != 3 {
		t.Errorf("got %v", blocked)
	}
	for item, want := range map[string]bool{
		"http://www.wikidata.org/entity/Q42":   false,
		"http://www.wikidata.org/entity/Q4":    true,
		"http://www.wikidata.org/entity/Q1000": false,
		"http://www.wikidata.org/entity/Q100":  true,
		"not an item":                          false,
	} {
		if got := blocked.allows(item); got != want {
			t.Errorf("allows(%q) = %v, want %v", item, got, want)
		}
	}

	for _, body := range []string{`{}`, `{"query":{"pages":{"103442925":{"missing":""}}}}`, `not json`} {
		if _, err := parseBlocklist([]byte(body)); err == nil {
			t.Errorf("%s should be an error", body)
		}
	}
}
