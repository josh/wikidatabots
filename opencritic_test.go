package main

import (
	"slices"
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }

const testItem = "http://www.wikidata.org/entity/Q1"
const testStatement = "http://www.wikidata.org/entity/statement/Q1-ABC"

func testGame() opencriticGame {
	return opencriticGame{
		topCriticScore:     ptr[float32](87.4),
		percentRecommended: ptr[float32](92.6),
		latestReviewDate:   ptr[int32](19723), // 2024-01-01
		retrievedOn:        ptr[int64](20089), // 2025-01-01
		numReviews:         ptr[uint16](120),
	}
}

func TestOpencriticAddStatement(t *testing.T) {
	reviews := []opencriticReview{{item: testItem, opencriticID: 7}}
	games := map[uint32]opencriticGame{7: testGame()}
	got, err := opencriticStatements(opencriticMethods[0], reviews, games, blocklist{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := `
<http://www.wikidata.org/entity/Q1> p:P444 [
  ps:P444 "87/100";
  pqe:P447 wd:Q21039459;
  pqe:P459 wd:Q114712322;
  pqe:P585 "2024-01-01"^^xsd:date;
  pqve:P7887 [
    rdf:type wikibase:QuantityValue;
    wikibase:quantityAmount "120"^^xsd:decimal;
    wikibase:quantityUnit wd:Q80698083
  ];
  prov:wasOnlyDerivedFrom [
    pr:P248 wd:Q21039459;
    pr:P2864 "7";
    pr:P813 "2025-01-01"^^xsd:date
  ];
  wikidatabots:editSummary "Add OpenCritic review score"
].
`
	if !slices.Equal(got, []string{want}) {
		t.Errorf("got %q", got)
	}
}

func TestOpencriticUpdateStatement(t *testing.T) {
	reviews := []opencriticReview{{
		item:         testItem,
		opencriticID: 7,
		statement:    testStatement,
		reviewScore:  "90%",
		numReviews:   ptr[uint16](120),
	}}
	games := map[uint32]opencriticGame{7: testGame()}
	got, err := opencriticStatements(opencriticMethods[1], reviews, games, blocklist{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := `
<http://www.wikidata.org/entity/statement/Q1-ABC>
  ps:P444 "93%";
  pqe:P447 wd:Q21039459;
  pqe:P459 wd:Q119576498;
  pqe:P585 "2024-01-01"^^xsd:date;
  pqve:P7887 [
    rdf:type wikibase:QuantityValue;
    wikibase:quantityAmount "120"^^xsd:decimal;
    wikibase:quantityUnit wd:Q80698083
  ];
  prov:wasOnlyDerivedFrom [
    pr:P248 wd:Q21039459;
    pr:P2864 "7";
    pr:P813 "2025-01-01"^^xsd:date
  ];
  wikidatabots:editSummary "Update OpenCritic review score".
`
	if !slices.Equal(got, []string{want}) {
		t.Errorf("got %q", got)
	}
}

func TestOpencriticChangeDetection(t *testing.T) {
	current := opencriticReview{
		item:         testItem,
		opencriticID: 7,
		statement:    testStatement,
		reviewScore:  "87/100",
		numReviews:   ptr[uint16](110),
	}
	tests := []struct {
		name   string
		review func(opencriticReview) opencriticReview
		game   func(opencriticGame) opencriticGame
		blocks blocklist
		emit   bool
	}{
		{name: "up to date"},
		{name: "review count at threshold", review: func(r opencriticReview) opencriticReview { r.numReviews = ptr[uint16](110); return r }},
		{name: "review count past threshold", review: func(r opencriticReview) opencriticReview { r.numReviews = ptr[uint16](109); return r }, emit: true},
		{name: "review count missing", review: func(r opencriticReview) opencriticReview { r.numReviews = nil; return r }, emit: true},
		{name: "score differs", review: func(r opencriticReview) opencriticReview { r.reviewScore = "86/100"; return r }, emit: true},
		{name: "score missing", review: func(r opencriticReview) opencriticReview { r.reviewScore = ""; return r }, emit: true},
		{name: "blocked", review: func(r opencriticReview) opencriticReview { r.reviewScore = ""; return r }, blocks: blocklist{"Q1": {}}},
		{name: "item without QID", review: func(r opencriticReview) opencriticReview { r.item, r.reviewScore = "http://example.com/", ""; return r }},
		{name: "game not in index", review: func(r opencriticReview) opencriticReview { r.opencriticID, r.reviewScore = 8, ""; return r }},
		{name: "no score", review: func(r opencriticReview) opencriticReview { r.reviewScore = ""; return r }, game: func(g opencriticGame) opencriticGame { g.topCriticScore = nil; return g }},
		{name: "no review date", review: func(r opencriticReview) opencriticReview { r.reviewScore = ""; return r }, game: func(g opencriticGame) opencriticGame { g.latestReviewDate = nil; return g }},
		{name: "no reviews", review: func(r opencriticReview) opencriticReview { r.reviewScore = ""; return r }, game: func(g opencriticGame) opencriticGame { g.numReviews = ptr[uint16](0); return g }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			review, game := current, testGame()
			if tt.review != nil {
				review = tt.review(review)
			}
			if tt.game != nil {
				game = tt.game(game)
			}
			got, err := opencriticStatements(opencriticMethods[0], []opencriticReview{review}, map[uint32]opencriticGame{7: game}, tt.blocks, 10)
			if err != nil {
				t.Fatal(err)
			}
			if (len(got) == 1) != tt.emit {
				t.Errorf("emitted %d statements, want emit=%v", len(got), tt.emit)
			}
		})
	}
}

func TestOpencriticScoreRounding(t *testing.T) {
	tests := []struct {
		method opencriticMethod
		score  float32
		want   string // empty means no statement
	}{
		{opencriticMethods[0], 72.5, `"72/100"`},
		{opencriticMethods[0], 73.5, `"74/100"`},
		{opencriticMethods[0], 72.51, `"73/100"`},
		{opencriticMethods[0], 0, `"0/100"`},
		{opencriticMethods[1], 99.5, `"100%"`},
		{opencriticMethods[1], 0.4, `"0%"`},
		{opencriticMethods[1], 0, ""},
	}
	for _, tt := range tests {
		game := testGame()
		game.topCriticScore, game.percentRecommended = &tt.score, &tt.score
		got, err := opencriticStatements(tt.method, []opencriticReview{{item: testItem, opencriticID: 7}}, map[uint32]opencriticGame{7: game}, blocklist{}, 10)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case tt.want == "" && len(got) != 0:
			t.Errorf("%v: unexpected statement", tt.score)
		case tt.want != "" && (len(got) != 1 || !strings.Contains(got[0], "ps:P444 "+tt.want+";")):
			t.Errorf("%v: want score %s in %q", tt.score, tt.want, got)
		}
	}

	game := testGame()
	game.topCriticScore = ptr[float32](300)
	if _, err := opencriticStatements(opencriticMethods[0], []opencriticReview{{item: testItem, opencriticID: 7}}, map[uint32]opencriticGame{7: game}, blocklist{}, 10); err == nil {
		t.Error("score above 255 should be an error")
	}
}

func TestParseOpencriticReviews(t *testing.T) {
	rows := [][]string{
		{"http://www.wikidata.org/entity/Q1", "0007", "", "", "", ""},
		{"http://www.wikidata.org/entity/Q2", "8", "http://www.wikidata.org/entity/statement/Q2-A", "80/100", "2024-01-01T00:00:00Z", "85"},
		{"http://www.wikidata.org/entity/Q3", "9", "http://www.wikidata.org/entity/statement/Q3-A", "70/100", "", "12.0"},
		{"http://www.wikidata.org/entity/Q3", "9", "http://www.wikidata.org/entity/statement/Q3-B", "71/100", "", ""},
	}
	reviews, err := parseOpencriticReviews(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(reviews) != 2 {
		t.Fatalf("got %d reviews, want 2 (Q3 has two statements)", len(reviews))
	}
	if r := reviews[0]; r.opencriticID != 7 || r.statement != "" || r.numReviews != nil {
		t.Errorf("Q1: %+v", r)
	}
	if r := reviews[1]; r.opencriticID != 8 || r.reviewScore != "80/100" || r.numReviews == nil || *r.numReviews != 85 {
		t.Errorf("Q2: %+v", r)
	}

	for _, bad := range [][]string{
		{"http://www.wikidata.org/entity/Q1", "abc", "", "", "", ""},
		{"http://www.wikidata.org/entity/Q1", "1", "", "", "yesterday", ""},
		{"http://www.wikidata.org/entity/Q1", "1", "", "", "", "70000"},
		{"http://www.wikidata.org/entity/Q1", "1", "", "", "", "many"},
	} {
		if _, err := parseOpencriticReviews([][]string{bad}); err == nil {
			t.Errorf("row %q should be an error", bad)
		}
	}
}

func TestFormatDate(t *testing.T) {
	for days, want := range map[int64]string{
		0:       "1970-01-01",
		-1:      "1969-12-31",
		19723:   "2024-01-01",
		19782:   "2024-02-29",
		-719162: "0001-01-01",
	} {
		if got := formatDate(days); got != want {
			t.Errorf("formatDate(%d) = %s, want %s", days, got, want)
		}
	}
	if got := floorDiv(-1, 86_400_000); got != -1 {
		t.Errorf("floorDiv(-1, day) = %d, want -1", got)
	}
}
