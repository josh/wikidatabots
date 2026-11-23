package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

func downloadParquet(ctx context.Context, url string) (*parquet.File, error) {
	start := time.Now()
	_, body, err := get(ctx, downloadClient, url, http.StatusOK)
	if err != nil {
		return nil, err
	}
	file, err := parquet.OpenFile(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", url, err)
	}
	log.Printf("parquet: %d rows, %.1f MB in %.1fs: %s", file.NumRows(), float64(len(body))/(1<<20), time.Since(start).Seconds(), url)
	return file, nil
}

type column struct {
	name string
	kind parquet.Kind
}

// scanColumns calls fn once per row with the values of the named top-level
// columns, in that order. Only those columns are decoded, and each must have
// the stated physical type. The slice passed to fn is reused between calls.
func scanColumns(file *parquet.File, columns []column, fn func(row []parquet.Value) error) error {
	indexes := make([]int, len(columns))
	for i, column := range columns {
		leaf, ok := file.Schema().Lookup(column.name)
		if !ok {
			return fmt.Errorf("parquet: missing column %q", column.name)
		}
		if kind := leaf.Node.Type().Kind(); kind != column.kind {
			return fmt.Errorf("parquet: column %q is %v, want %v", column.name, kind, column.kind)
		}
		indexes[i] = leaf.ColumnIndex
	}

	row := make([]parquet.Value, len(columns))
	for _, group := range file.RowGroups() {
		chunks := group.ColumnChunks()
		cursors := make([]*columnCursor, len(columns))
		for i, index := range indexes {
			cursors[i] = &columnCursor{pages: chunks[index].Pages()}
		}
		err := func() error {
			for range group.NumRows() {
				for i, cursor := range cursors {
					value, err := cursor.next()
					if err != nil {
						return fmt.Errorf("parquet: column %q: %w", columns[i].name, err)
					}
					row[i] = value
				}
				if err := fn(row); err != nil {
					return err
				}
			}
			return nil
		}()
		for _, cursor := range cursors {
			cursor.close()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// timestampUnit returns the duration of one tick of a timestamp column.
func timestampUnit(file *parquet.File, name string) (time.Duration, error) {
	leaf, ok := file.Schema().Lookup(name)
	if !ok {
		return 0, fmt.Errorf("parquet: missing column %q", name)
	}
	logical := leaf.Node.Type().String()
	if strings.HasPrefix(logical, "TIMESTAMP") {
		switch {
		case strings.Contains(logical, "MILLIS"):
			return time.Millisecond, nil
		case strings.Contains(logical, "MICROS"):
			return time.Microsecond, nil
		case strings.Contains(logical, "NANOS"):
			return time.Nanosecond, nil
		}
	}
	return 0, fmt.Errorf("parquet: column %q is %s, want a timestamp", name, logical)
}

type columnCursor struct {
	pages  parquet.Pages
	page   parquet.Page
	values parquet.ValueReader
	buffer [1024]parquet.Value
	pos, n int
}

func (c *columnCursor) next() (parquet.Value, error) {
	for c.pos == c.n {
		if c.values == nil {
			page, err := c.pages.ReadPage()
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = io.ErrUnexpectedEOF
				}
				return parquet.Value{}, err
			}
			c.page, c.values = page, page.Values()
		}
		n, err := c.values.ReadValues(c.buffer[:])
		c.pos, c.n = 0, n
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return parquet.Value{}, err
			}
			parquet.Release(c.page)
			c.page, c.values = nil, nil
		}
	}
	value := c.buffer[c.pos]
	c.pos++
	return value, nil
}

func (c *columnCursor) close() {
	if c.page != nil {
		parquet.Release(c.page)
	}
	c.pages.Close()
}
