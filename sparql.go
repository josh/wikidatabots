package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const sparqlEndpoint = "https://query.wikidata.org/sparql"

var (
	sparqlClient = &http.Client{
		Timeout: 10 * time.Minute,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 90 * time.Second,
		},
	}
	sparqlBackoff = backoff{maxTries: 30, base: time.Second, maxWait: 120 * time.Second}
)

// sparqlQuery runs query against the Wikidata Query Service and returns one
// slice per result row, with fields in the order of columns. Unbound variables
// are empty strings.
func sparqlQuery(ctx context.Context, query string, columns ...string) ([][]string, error) {
	oneLine := strings.Join(strings.Fields(query), " ")
	start := time.Now()
	body, err := retry(ctx, "sparql", sparqlBackoff, func() ([]byte, error) {
		return sparqlPost(ctx, query)
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, oneLine)
	}

	reader := csv.NewReader(bytes.NewReader(body))
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("sparql: reading csv header: %w", err)
	}
	index := make([]int, len(columns))
	for i, column := range columns {
		index[i] = slices.Index(header, column)
		if index[i] < 0 {
			return nil, fmt.Errorf("sparql: result has no column %q", column)
		}
	}

	var rows [][]string
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("sparql: reading csv: %w", err)
		}
		row := make([]string, len(index))
		for i, j := range index {
			row[i] = record[j]
		}
		rows = append(rows, row)
	}
	log.Printf("sparql: %d rows in %.1fs: %s", len(rows), time.Since(start).Seconds(), oneLine)
	return rows, nil
}

func sparqlPost(ctx context.Context, query string) ([]byte, error) {
	form := url.Values{"query": {query}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sparqlEndpoint, strings.NewReader(form))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "text/csv")
	req.Header.Set("User-Agent", userAgent)
	resp, err := sparqlClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// The service can report a timeout part-way through a 200 response.
	if bytes.Contains(body, []byte("java.util.concurrent.TimeoutException")) {
		return nil, errors.New("query timed out")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}
	return body, nil
}
