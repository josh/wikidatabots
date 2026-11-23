package main

import (
	"bufio"
	"io"
	"log"
	"math/rand/v2"
)

// writeStatements prints one statement per line. Above limit it keeps a random
// sample, so a large backlog is worked through evenly across runs.
func writeStatements(w io.Writer, statements []string, limit int) error {
	if limit > 0 && len(statements) > limit {
		log.Printf("warning: rdf statements exceeded limit: %d/%d", len(statements), limit)
		sampled := make([]string, 0, limit)
		for _, i := range rand.Perm(len(statements))[:limit] {
			sampled = append(sampled, statements[i])
		}
		statements = sampled
	}

	out := bufio.NewWriter(w)
	for _, statement := range statements {
		out.WriteString(statement)
		out.WriteByte('\n')
	}
	return out.Flush()
}
