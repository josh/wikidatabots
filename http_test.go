package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

var fastBackoff = backoff{maxTries: 3, base: time.Millisecond}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	calls := 0
	got, err := retry(context.Background(), "test", fastBackoff, func() (int, error) {
		calls++
		if calls < 3 {
			return 0, errors.New("boom")
		}
		return 42, nil
	})
	if err != nil || got != 42 || calls != 3 {
		t.Fatalf("got %d, %v after %d calls", got, err, calls)
	}
}

func TestRetryGivesUp(t *testing.T) {
	calls := 0
	_, err := retry(context.Background(), "test", fastBackoff, func() (int, error) {
		calls++
		return 0, errors.New("boom")
	})
	if err == nil || calls != 3 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
	if !strings.Contains(err.Error(), "giving up after 3 tries: boom") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRetryStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := retry(ctx, "test", backoff{maxTries: 5, base: time.Hour}, func() (int, error) {
		calls++
		cancel()
		return 0, errors.New("boom")
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
}

func TestRetryCapsWait(t *testing.T) {
	start := time.Now()
	b := backoff{maxTries: 40, base: time.Millisecond, maxWait: 2 * time.Millisecond}
	retry(context.Background(), "test", b, func() (int, error) { return 0, errors.New("boom") })
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("40 tries capped at 2ms each took %v", elapsed)
	}
}

const echoURL = "https://postman-echo.com"

func TestGet(t *testing.T) {
	if testing.Short() {
		t.Skip("network")
	}
	ctx := context.Background()

	status, body, err := get(ctx, apiClient, echoURL+"/headers", http.StatusOK)
	if err != nil || status != 200 || !strings.Contains(string(body), `"user-agent":"Josh404Bot/1.0 (User:Josh404Bot) Go/`) {
		t.Errorf("ok: %d %q %v", status, body, err)
	}

	status, _, err = get(ctx, apiClient, echoURL+"/status/404", http.StatusOK, http.StatusNotFound)
	if err != nil || status != 404 {
		t.Errorf("allowed 404: %d %v", status, err)
	}

	if _, _, err = get(ctx, apiClient, echoURL+"/status/404", http.StatusOK); err == nil {
		t.Error("unexpected 404 should be an error")
	}

	redirect := echoURL + "/redirect-to?url=" + url.QueryEscape(echoURL+"/get")
	if status, _, err = get(ctx, apiClient, redirect, http.StatusOK); err == nil || status != 302 {
		t.Errorf("redirect should not be followed: %d %v", status, err)
	}
}

func TestGetErrorsHideQueryString(t *testing.T) {
	ctx := context.Background()

	_, _, err := get(ctx, apiClient, "http://127.0.0.1:1/3/movie/1?api_key=SECRET", http.StatusOK)
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Errorf("connection error leaks the key: %v", err)
	}

	_, _, err = get(ctx, apiClient, "http://bad host/?api_key=SECRET", http.StatusOK)
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Errorf("invalid URL error leaks the key: %v", err)
	}

	if testing.Short() {
		t.Skip("network")
	}
	_, _, err = get(ctx, apiClient, echoURL+"/status/404?api_key=SECRET", http.StatusOK)
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Errorf("status error leaks the key: %v", err)
	}
}

func TestTMDBClientStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("network")
	}
	client, err := newTMDBClient(echoURL, "SECRET")
	if err != nil {
		t.Fatal(err)
	}
	client.backoff.base = time.Millisecond
	ctx := context.Background()

	if _, err := client.get(ctx, "/status/404"); err != nil {
		t.Errorf("404 should not be an error: %v", err)
	}
	for _, status := range []string{"429", "500"} {
		_, err := client.get(ctx, "/status/"+status)
		if err == nil || !strings.Contains(err.Error(), "giving up after 3 tries") || strings.Contains(err.Error(), "SECRET") {
			t.Errorf("%s: %v", status, err)
		}
	}
	if _, _, err := client.find(ctx, tmdbMovie, "imdb_id", "tt1630029"); err == nil {
		t.Error("a response that is not JSON should be an error")
	}
}
