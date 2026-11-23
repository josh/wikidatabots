// Command wikidatabots prints Wikidata edits as RDF statements for
// wikidata-rdf-patch to apply.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const defaultStatementLimit = 250

type bot func(ctx context.Context) ([]string, error)

var bots = map[string]bot{
	"opencritic": opencriticBot,
	"tmdb":       tmdbBot,
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	log.SetOutput(stderr)

	flags := flag.NewFlagSet("wikidatabots", flag.ContinueOnError)
	flags.SetOutput(stderr)
	limit := flags.Int("limit", defaultStatementLimit, "maximum statements to print, sampled at random when exceeded; 0 prints all")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "usage: wikidatabots [-limit n] <opencritic|tmdb>")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return 2
	}
	selected, ok := bots[flags.Arg(0)]
	if flags.NArg() != 1 || !ok || *limit < 0 {
		flags.Usage()
		return 2
	}

	// Statements are written only after the whole run succeeds, so a failure
	// never leaves a partial patch on stdout.
	statements, err := selected(ctx)
	if err != nil {
		log.Printf("error: %v", err)
		return 1
	}
	if err := writeStatements(stdout, statements, *limit); err != nil {
		log.Printf("error: %v", err)
		return 1
	}
	return 0
}
