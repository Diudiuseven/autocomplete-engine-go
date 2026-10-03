package engine

import (
	"context"
	"fmt"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/tenant"
	"testing"
	"time"
)

// TestQueryTimeout: an impossible deadline returns a distinguishable error,
// never partial results.
func TestQueryTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = ""
	cfg.QueryTimeout = 1 * time.Nanosecond
	eng, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateTenant("t1", tenant.Quota{}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := eng.Upsert(ctx, "t1", fmt.Sprintf("term%04d", i), 1, 1, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	res, err := eng.Search(ctx, "t1", "term", SearchOptions{})
	if err != ErrQueryTimeout {
		t.Fatalf("want ErrQueryTimeout, got %v (res=%v)", err, res)
	}
	if res != nil {
		t.Fatal("timeout must not return partial results")
	}
}

// TestScanLimit: exceeding the scan cap returns a distinguishable error.
func TestScanLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = ""
	cfg.ScanLimit = 5
	cfg.QueryTimeout = 2 * time.Second
	eng, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateTenant("t1", tenant.Quota{}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := eng.Upsert(ctx, "t1", fmt.Sprintf("term%04d", i), 1, 1, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	// "term" matches everything -> scan cap exceeded
	if _, err := eng.Search(ctx, "t1", "term", SearchOptions{}); err != ErrScanLimit {
		t.Fatalf("want ErrScanLimit, got %v", err)
	}
	// a query that matches nothing still scans everything -> also capped
	if _, err := eng.Search(ctx, "t1", "zzz", SearchOptions{}); err != ErrScanLimit {
		t.Fatalf("want ErrScanLimit for full scan, got %v", err)
	}
}

// TestResultCap: MaxResults truncates to the top-N deterministically.
func TestResultCap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = ""
	cfg.MaxResults = 3
	cfg.QueryTimeout = 2 * time.Second
	eng, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateTenant("t1", tenant.Quota{}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := eng.Upsert(ctx, "t1", fmt.Sprintf("item%02d", i), float64(i+1), 1, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	res, err := eng.Search(ctx, "t1", "item", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Matches) != 3 {
		t.Fatalf("want 3 matches, got %d", len(res.Matches))
	}
	// highest heat first
	if res.Matches[0].Term != "item09" {
		t.Fatalf("top result = %s, want item09", res.Matches[0].Term)
	}
	// per-query override
	res, _ = eng.Search(ctx, "t1", "item", SearchOptions{Limit: 5})
	if len(res.Matches) != 5 {
		t.Fatalf("override limit: got %d", len(res.Matches))
	}
}
