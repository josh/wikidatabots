package main

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	opencriticIndexURL = "https://josh.github.io/opencritic-index/opencritic.parquet"

	// A statement whose score is unchanged is refreshed only once the review
	// count has grown by more than this.
	opencriticNumReviewsThreshold = 10
)

const opencriticQuery = `
SELECT ?item ?opencritic_id ?statement
      ?review_score ?point_in_time ?number_of_reviews WHERE {
  ?item wdt:P2864 ?opencritic_id.
  FILTER(xsd:integer(?opencritic_id))

  OPTIONAL {
    ?item p:P444 ?statement.

    ?statement wikibase:rank ?rank.
    FILTER(?rank != wikibase:DeprecatedRank)

    ?statement ps:P444 ?review_score.
    ?statement pq:P447 wd:Q21039459.

    ?statement pq:P459 ?determination_method.
    OPTIONAL { ?statement pq:P585 ?point_in_time. }
    OPTIONAL { ?statement pq:P7887 ?number_of_reviews. }
  }
}
`

// Arguments: subject, score, method QID, latest review date, number of
// reviews, OpenCritic ID, retrieved date.
const opencriticAddTemplate = `
<%s> p:P444 [
  ps:P444 "%s";
  pqe:P447 wd:Q21039459;
  pqe:P459 wd:%s;
  pqe:P585 "%s"^^xsd:date;
  pqve:P7887 [
    rdf:type wikibase:QuantityValue;
    wikibase:quantityAmount "%d"^^xsd:decimal;
    wikibase:quantityUnit wd:Q80698083
  ];
  prov:wasOnlyDerivedFrom [
    pr:P248 wd:Q21039459;
    pr:P2864 "%d";
    pr:P813 "%s"^^xsd:date
  ];
  wikidatabots:editSummary "Add OpenCritic review score"
].
`

const opencriticUpdateTemplate = `
<%s>
  ps:P444 "%s";
  pqe:P447 wd:Q21039459;
  pqe:P459 wd:%s;
  pqe:P585 "%s"^^xsd:date;
  pqve:P7887 [
    rdf:type wikibase:QuantityValue;
    wikibase:quantityAmount "%d"^^xsd:decimal;
    wikibase:quantityUnit wd:Q80698083
  ];
  prov:wasOnlyDerivedFrom [
    pr:P248 wd:Q21039459;
    pr:P2864 "%d";
    pr:P813 "%s"^^xsd:date
  ];
  wikidatabots:editSummary "Update OpenCritic review score".
`

// opencriticMethod is one "determination method" (P459) the bot maintains a
// review score statement for.
type opencriticMethod struct {
	qid         string
	scoreFormat string
	score       func(opencriticGame) *float32
	// positiveOnly skips games whose score is zero.
	positiveOnly bool
}

var opencriticMethods = []opencriticMethod{
	{
		qid:         "Q114712322", // top critic score
		scoreFormat: "%d/100",
		score:       func(g opencriticGame) *float32 { return g.topCriticScore },
	},
	{
		qid:          "Q119576498", // percent recommended
		scoreFormat:  "%d%%",
		score:        func(g opencriticGame) *float32 { return g.percentRecommended },
		positiveOnly: true,
	},
}

// opencriticGame is a row of the OpenCritic index.
type opencriticGame struct {
	topCriticScore     *float32
	percentRecommended *float32
	latestReviewDate   *int32 // days since the Unix epoch
	retrievedOn        *int64 // days since the Unix epoch
	numReviews         *uint16
}

// opencriticReview is what Wikidata currently says about an item: its
// OpenCritic ID and, if present, the existing review score statement.
type opencriticReview struct {
	item         string
	opencriticID uint32
	statement    string
	reviewScore  string
	numReviews   *uint16
}

func opencriticBot(ctx context.Context) ([]string, error) {
	blocked, err := fetchBlocklist(ctx)
	if err != nil {
		return nil, err
	}
	games, err := loadOpencriticGames(ctx)
	if err != nil {
		return nil, err
	}

	var statements []string
	for _, method := range opencriticMethods {
		query := strings.Replace(opencriticQuery, "?determination_method", "wd:"+method.qid, 1)
		rows, err := sparqlQuery(ctx, query, "item", "opencritic_id", "statement", "review_score", "point_in_time", "number_of_reviews")
		if err != nil {
			return nil, err
		}
		reviews, err := parseOpencriticReviews(rows)
		if err != nil {
			return nil, err
		}
		s, err := opencriticStatements(method, reviews, games, blocked, opencriticNumReviewsThreshold)
		if err != nil {
			return nil, err
		}
		statements = append(statements, s...)
	}
	return statements, nil
}

func loadOpencriticGames(ctx context.Context) (map[uint32]opencriticGame, error) {
	file, err := downloadParquet(ctx, opencriticIndexURL)
	if err != nil {
		return nil, err
	}
	unit, err := timestampUnit(file, "retrieved_at")
	if err != nil {
		return nil, err
	}
	ticksPerDay := int64(24 * time.Hour / unit)

	games := make(map[uint32]opencriticGame, file.NumRows())
	columns := []column{
		{"id", parquet.Int32},
		{"top_critic_score", parquet.Float},
		{"percent_recommended", parquet.Float},
		{"latest_review_date", parquet.Int32},
		{"retrieved_at", parquet.Int64},
		{"num_reviews", parquet.Int32},
	}
	err = scanColumns(file, columns, func(row []parquet.Value) error {
		if row[0].IsNull() {
			return nil
		}
		var game opencriticGame
		if v := row[1]; !v.IsNull() {
			f := v.Float()
			game.topCriticScore = &f
		}
		if v := row[2]; !v.IsNull() {
			f := v.Float()
			game.percentRecommended = &f
		}
		if v := row[3]; !v.IsNull() {
			d := v.Int32()
			game.latestReviewDate = &d
		}
		if v := row[4]; !v.IsNull() {
			d := floorDiv(v.Int64(), ticksPerDay)
			game.retrievedOn = &d
		}
		if v := row[5]; !v.IsNull() {
			n := v.Int32()
			if n < 0 || n > math.MaxUint16 {
				return fmt.Errorf("opencritic index: num_reviews %d out of range", n)
			}
			u := uint16(n)
			game.numReviews = &u
		}
		games[uint32(row[0].Int32())] = game
		return nil
	})
	return games, err
}

// parseOpencriticReviews reads SPARQL rows of item, opencritic_id, statement,
// review_score, point_in_time, number_of_reviews. Items with more than one row
// have several candidate statements, so they are left for a human.
func parseOpencriticReviews(rows [][]string) ([]opencriticReview, error) {
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		counts[row[0]]++
	}

	var reviews []opencriticReview
	for _, row := range rows {
		review := opencriticReview{item: row[0], statement: row[2], reviewScore: row[3]}
		if row[1] == "" {
			continue
		}
		id, err := strconv.ParseUint(row[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%s: OpenCritic ID: %w", review.item, err)
		}
		review.opencriticID = uint32(id)
		if row[4] != "" {
			if _, err := time.Parse(time.RFC3339, row[4]); err != nil {
				return nil, fmt.Errorf("%s: point in time: %w", review.item, err)
			}
		}
		if row[5] != "" {
			n, err := strconv.ParseFloat(row[5], 64)
			if err != nil {
				return nil, fmt.Errorf("%s: number of reviews: %w", review.item, err)
			}
			if !(n > -1 && n < math.MaxUint16+1) {
				return nil, fmt.Errorf("%s: number of reviews %q out of range", review.item, row[5])
			}
			u := uint16(n)
			review.numReviews = &u
		}
		if counts[review.item] == 1 {
			reviews = append(reviews, review)
		}
	}
	return reviews, nil
}

func opencriticStatements(method opencriticMethod, reviews []opencriticReview, games map[uint32]opencriticGame, blocked blocklist, numReviewsThreshold int) ([]string, error) {
	var statements []string
	for _, review := range reviews {
		if !blocked.allows(review.item) {
			continue
		}
		game, ok := games[review.opencriticID]
		if !ok {
			continue
		}
		score := method.score(game)
		if score == nil || game.latestReviewDate == nil || game.retrievedOn == nil || game.numReviews == nil || *game.numReviews == 0 {
			continue
		}
		if method.positiveOnly && *score <= 0 {
			continue
		}
		rounded := math.RoundToEven(float64(*score))
		if !(rounded >= 0 && rounded <= math.MaxUint8) {
			return nil, fmt.Errorf("opencritic index: score %v for game %d out of range", *score, review.opencriticID)
		}
		reviewScore := fmt.Sprintf(method.scoreFormat, uint8(rounded))

		current := review.reviewScore == reviewScore &&
			review.numReviews != nil &&
			int(*review.numReviews)+numReviewsThreshold >= int(*game.numReviews)
		if current {
			continue
		}

		template, subject := opencriticUpdateTemplate, review.statement
		if review.statement == "" {
			template, subject = opencriticAddTemplate, review.item
		}
		statements = append(statements, fmt.Sprintf(template,
			subject,
			reviewScore,
			method.qid,
			formatDate(int64(*game.latestReviewDate)),
			*game.numReviews,
			review.opencriticID,
			formatDate(*game.retrievedOn),
		))
	}
	return statements, nil
}

// formatDate renders days since the Unix epoch as YYYY-MM-DD.
func formatDate(days int64) string {
	return time.Unix(days*86400, 0).UTC().Format(time.DateOnly)
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b < 0 {
		q--
	}
	return q
}
