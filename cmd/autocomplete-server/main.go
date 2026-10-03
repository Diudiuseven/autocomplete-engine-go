// Command autocomplete-server runs the autocomplete HTTP service.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/Diudiuseven/autocomplete-engine-go/api/httpapi"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/engine"
)

func main() {
	var (
		addr          = flag.String("addr", ":8080", "listen address")
		dataDir       = flag.String("data-dir", "./data", "persistence directory (empty disables persistence)")
		maxResults    = flag.Int("max-results", 20, "max candidates returned per query")
		scanLimit     = flag.Int("scan-limit", 10000, "max candidates examined per query")
		queryTimeout  = flag.Duration("query-timeout", 200*time.Millisecond, "query timeout")
		maxQueryLen   = flag.Int("max-query-len", 64, "max query length in runes")
		heatHalfLife  = flag.Duration("heat-half-life", 30*24*time.Hour, "heat decay half-life")
		recencyHalf   = flag.Duration("recency-half-life", 3*24*time.Hour, "recency boost half-life")
		recencyWeight = flag.Float64("recency-weight", 1.0, "recency boost weight")
	)
	flag.Parse()

	cfg := engine.Config{
		DataDir:         *dataDir,
		MaxResults:      *maxResults,
		ScanLimit:       *scanLimit,
		QueryTimeout:    *queryTimeout,
		MaxQueryLen:     *maxQueryLen,
		HeatHalfLife:    *heatHalfLife,
		RecencyHalfLife: *recencyHalf,
		RecencyWeight:   *recencyWeight,
	}
	eng, err := engine.New(cfg)
	if err != nil {
		log.Fatalf("engine init: %v", err)
	}
	defer eng.Close()

	srv := httpapi.New(eng)
	log.Printf("autocomplete-server listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv.Handler()))
}
