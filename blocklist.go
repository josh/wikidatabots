package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"
)

// The blocklist is the page User:Josh404Bot/Blocklist; every QID mentioned on
// it is off limits to the bots.
const (
	blocklistPageID = "103442925"
	blocklistURL    = "https://www.wikidata.org/w/api.php?action=query&format=json&pageids=" + blocklistPageID + "&prop=extracts&explaintext=1"
)

var (
	qidPattern       = regexp.MustCompile(`Q[0-9]+`)
	blocklistBackoff = backoff{maxTries: 3, base: time.Second, maxTime: 300 * time.Second}
)

type blocklist map[string]struct{}

func fetchBlocklist(ctx context.Context) (blocklist, error) {
	body, err := retry(ctx, "blocklist", blocklistBackoff, func() ([]byte, error) {
		_, body, err := get(ctx, apiClient, blocklistURL, http.StatusOK)
		return body, err
	})
	if err != nil {
		return nil, err
	}
	return parseBlocklist(body)
}

func parseBlocklist(body []byte) (blocklist, error) {
	var response struct {
		Query struct {
			Pages map[string]struct {
				Extract *string `json:"extract"`
			} `json:"pages"`
		} `json:"query"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("blocklist: %w", err)
	}
	extract := response.Query.Pages[blocklistPageID].Extract
	if extract == nil {
		// Running without a blocklist would edit items the bot was told to
		// leave alone, so a missing page is fatal.
		return nil, errors.New("blocklist: page extract missing")
	}
	blocked := blocklist{}
	for _, qid := range qidPattern.FindAllString(*extract, -1) {
		blocked[qid] = struct{}{}
	}
	return blocked, nil
}

// allows reports whether the item URI names a QID that is not blocked.
func (b blocklist) allows(item string) bool {
	qid := qidPattern.FindString(item)
	if qid == "" {
		return false
	}
	_, blocked := b[qid]
	return !blocked
}
