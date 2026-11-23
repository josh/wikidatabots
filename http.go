package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"runtime"
	"slices"
	"strings"
	"time"
)

var userAgent = "Josh404Bot/1.0 (User:Josh404Bot) Go/" + strings.TrimPrefix(runtime.Version(), "go")

var (
	// apiClient never follows redirects: an unexpected 3xx is an error, not
	// a reason to send the request somewhere else.
	apiClient = &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	downloadClient = &http.Client{Timeout: 5 * time.Minute}
)

// backoff is exponential with full jitter: the wait before retry n is uniform
// in [0, min(base*2^(n-1), maxWait)].
type backoff struct {
	maxTries int
	base     time.Duration
	maxWait  time.Duration // zero means uncapped
	maxTime  time.Duration // zero means no overall deadline
}

func retry[T any](ctx context.Context, desc string, b backoff, fn func() (T, error)) (T, error) {
	start := time.Now()
	for try := 1; ; try++ {
		v, err := fn()
		if err == nil {
			return v, nil
		}
		if ctx.Err() != nil {
			return v, ctx.Err()
		}
		if try >= b.maxTries || (b.maxTime > 0 && time.Since(start) >= b.maxTime) {
			return v, fmt.Errorf("%s: giving up after %d tries: %w", desc, try, err)
		}
		wait := b.base << (try - 1)
		if b.maxWait > 0 && (wait > b.maxWait || wait <= 0) {
			wait = b.maxWait
		}
		wait = time.Duration(rand.Float64() * float64(wait))
		log.Printf("%s: %v; retrying in %.1fs", desc, err, wait.Seconds())
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return v, ctx.Err()
		}
	}
}

// get performs one GET and returns the status and body when the status is one
// of ok. Errors never contain the query string, which may carry an API key.
func get(ctx context.Context, client *http.Client, rawURL string, ok ...int) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("GET %s: invalid URL", redactURL(rawURL))
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, redactError(err)
	}
	defer resp.Body.Close()
	if !slices.Contains(ok, resp.StatusCode) {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return resp.StatusCode, nil, fmt.Errorf("GET %s: %s", redactURL(rawURL), resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("GET %s: %w", redactURL(rawURL), redactError(err))
	}
	return resp.StatusCode, body, nil
}

func redactURL(rawURL string) string {
	base, _, found := strings.Cut(rawURL, "?")
	if found {
		return base + "?…"
	}
	return base
}

func redactError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return &url.Error{Op: urlErr.Op, URL: redactURL(urlErr.URL), Err: urlErr.Err}
	}
	return err
}
