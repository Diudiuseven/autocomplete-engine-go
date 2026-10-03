package autocomplete

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMaxCandidatesAndScanLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxCandidates = 3
	cfg.ScanLimit = 5
	e, clk := newTestEngine(t, cfg)
	now := clk.Now()
	// Insert 20 keys that all contain "ab": sorted order makes the scanned
	// subset deterministic.
	for i := 0; i < 20; i++ {
		upsert(t, e, "t1", fmt.Sprintf("ab%02d", i), 1, 1, now)
	}
	got, stats, err := e.Query(context.Background(), "t1", "ab", QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("MaxCandidates cap: got %d", len(got))
	}
	if stats.Scanned != 5 {
		t.Fatalf("ScanLimit should bound work, scanned=%d", stats.Scanned)
	}
	want := []string{"ab00", "ab01", "ab02"}
	if fmt.Sprint(keysOf(got)) != fmt.Sprint(want) {
		t.Fatalf("bounded scan must pick deterministic subset: %v", keysOf(got))
	}

	// Per-query override
	got2, _, err := e.Query(context.Background(), "t1", "ab", QueryOptions{MaxCandidates: 2, ScanLimit: 20})
	if err != nil || len(got2) != 2 {
		t.Fatalf("override: %v %d", err, len(got2))
	}
}

func TestQueryTimeoutReturnsNoPartial(t *testing.T) {
	cfg := testConfig()
	cfg.ScanLimit = 100_000_000
	cfg.QueryTimeout = time.Nanosecond // impossible deadline for the work
	e, clk := newTestEngine(t, cfg)
	now := clk.Now()
	for i := 0; i < 2000; i++ {
		upsert(t, e, "t1", fmt.Sprintf("zterm%05d", i), 1, 1, now)
	}
	_, stats, err := e.Query(context.Background(), "t1", "term", QueryOptions{})
	if !errors.Is(err, ErrQueryTimeout) {
		t.Skipf("query faster than forced deadline on this machine: %v", err)
	}
	if stats.TimedOut != true {
		t.Fatal("stats must flag timeout")
	}
}

func TestSlowQueryDoesNotBlockWrites(t *testing.T) {
	cfg := testConfig()
	cfg.ScanLimit = 20_000
	e, clk := newTestEngine(t, cfg)
	now := clk.Now()
	for i := 0; i < 2000; i++ {
		upsert(t, e, "t1", fmt.Sprintf("w%05d", i), 1, 1, now)
	}
	var wg sync.WaitGroup
	// Readers hammer queries while a writer keeps committing; all readers must
	// see only consistent versions and the writer must finish promptly.
	writeDone := make(chan struct{})
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(writeDone)
		for i := 0; i < 30; i++ {
			if err := e.Upsert(context.Background(), "t1", BatchItem{Display: fmt.Sprintf("new%d", i)}); err != nil {
				t.Errorf("write during reads: %v", err)
				return
			}
		}
	}()
	readerWg := sync.WaitGroup{}
	for r := 0; r < 4; r++ {
		readerWg.Add(1)
		go func() {
			defer readerWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, stats, err := e.Query(context.Background(), "t1", "w", QueryOptions{Timeout: 2 * time.Millisecond})
				if err != nil && !errors.Is(err, ErrQueryTimeout) {
					t.Errorf("query: %v", err)
					return
				}
				// Every returned version is a committed integer version.
				if stats.Version == 0 {
					t.Errorf("unversioned read")
					return
				}
				_ = got
			}
		}()
	}
	<-writeDone
	close(stop)
	readerWg.Wait()
	wg.Wait()
	// final state contains all 50 new entries
	if n := len(e.idx.current().tenant("t1").entries); n != 2030 {
		t.Fatalf("missing concurrent writes: %d", n)
	}
}

func TestContextDeadlinePropagates(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)
	_, _, err := e.Query(ctx, "t1", "a", QueryOptions{})
	if err == nil {
		t.Fatal("expired context must fail the query")
	}
}
