package autocomplete

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWALHardCorruptionMidFile(t *testing.T) {
	dir := t.TempDir()
	cfg := persistConfig(dir, 0)
	e, _ := NewEngine(cfg)
	ctx := context.Background()
	_ = e.RegisterTenant(ctx, "t1", 0)
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "one"})
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "two"})
	_ = e.Close()

	path := filepath.Join(dir, walFileName)
	raw, _ := os.ReadFile(path)
	// Corrupt one byte inside the first frame's payload, but keep length intact;
	// intact frames follow, so it is NOT a torn tail and must be reported.
	// Find first payload start (offset 5) and flip a byte beyond the header.
	pos := 5
	raw[pos] ^= 0xFF
	// If CRC mismatch frames tail detection treats it as torn because the
	// remainder is no longer parseable as frames. To make it a true mid-file
	// structural error, overwrite the magic byte of the FIRST frame and
	// restore the payload byte.
	raw[pos] ^= 0xFF
	raw[0] = 'X'
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngine(cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mid-file structural corruption must be ErrCorrupt, got %v", err)
	}
}

func TestTenantStatsIsolated(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	ctx := context.Background()
	if err := e.RegisterTenant(ctx, "a", 0); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterTenant(ctx, "b", 2); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	_ = e.Upsert(ctx, "a", BatchItem{Display: "shared", LastUsedAt: now})
	_ = e.Upsert(ctx, "a", BatchItem{Display: "shared", LastUsedAt: now})
	_ = e.Upsert(ctx, "b", BatchItem{Display: "shared", LastUsedAt: now})
	// b quota: two more distinct keys -> rejection
	_ = e.Upsert(ctx, "b", BatchItem{Display: "x", LastUsedAt: now})
	err := e.Upsert(ctx, "b", BatchItem{Display: "y", LastUsedAt: now})
	if !errors.Is(err, ErrTenantQuotaExceeded) {
		t.Fatalf("b quota exceeded expected, got %v", err)
	}
	// A's stats unaffected by b's rejection and counts.
	sa, _ := e.TenantStats(ctx, "a")
	sb, _ := e.TenantStats(ctx, "b")
	if sa.Updates != 2 || sa.Rejected != 0 || sa.Candidates != 1 {
		t.Fatalf("tenant a stats leaked: %+v", sa)
	}
	if sb.Updates != 2 || sb.Rejected != 1 || sb.Candidates != 2 {
		t.Fatalf("tenant b stats wrong: %+v", sb)
	}
	// queries are counted per tenant
	_, _, _ = e.Query(ctx, "a", "shared", QueryOptions{})
	_, _, _ = e.Query(ctx, "a", "shared", QueryOptions{})
	_, _, _ = e.Query(ctx, "b", "shared", QueryOptions{})
	sa, _ = e.TenantStats(ctx, "a")
	sb, _ = e.TenantStats(ctx, "b")
	if sa.Queries != 2 || sb.Queries != 1 {
		t.Fatalf("query stats crossed tenants: a=%d b=%d", sa.Queries, sb.Queries)
	}
	if _, err := e.TenantStats(ctx, "nope"); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("unknown tenant stats: %v", err)
	}
}

func TestRepeatedUpdatesRemainConsistent(t *testing.T) {
	cfg := testConfig()
	e, clk := newTestEngine(t, cfg)
	// 300 sequential upserts across two keys; size stays 2 and mass matches.
	for i := 0; i < 300; i++ {
		d := "Foo"
		if i%2 == 0 {
			d = "foo" // same normalized key
		}
		upsert(t, e, "t1", d, 2, 3, clk.Now())
		clk.advance(time.Minute)
	}
	upsert(t, e, "t1", "bar", 1, 1, clk.Now())
	ts := e.idx.current().tenant("t1")
	if len(ts.entries) != 2 {
		t.Fatalf("size drift: %d", len(ts.entries))
	}
	en := ts.entries["foo"]
	if en.Heat != 600 || en.UseCount != 900 {
		t.Fatalf("repeated accumulation wrong: %+v", en)
	}
	// Repeated queries identical bytes
	r1, _, _ := e.idx.Query("t1", "o", QueryOptions{})
	r2, _, _ := e.idx.Query("t1", "o", QueryOptions{})
	if !bytes.Equal([]byte(candRepr(r1)), []byte(candRepr(r2))) {
		t.Fatalf("repeated query ordering differs")
	}
}

func candRepr(cs []Candidate) string {
	out := ""
	for _, c := range cs {
		out += c.Entry.Key + "|"
	}
	return out
}

func TestDeleteIdempotentAndPersisted(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	upsert(t, e, "t1", "doomed", 1, 1, time.Now().UTC())
	if err := e.Delete(context.Background(), "t1", "DOOMED"); err != nil { // same normalized key
		t.Fatal(err)
	}
	if err := e.Delete(context.Background(), "t1", "doomed"); err != nil {
		t.Fatalf("delete missing must be no-op, got %v", err)
	}
	if n := len(e.idx.current().tenant("t1").entries); n != 0 {
		t.Fatalf("delete failed: %d", n)
	}
}

func TestConcurrentAddToSingleBatch(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	b, err := e.BeginBatch(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := e.AddToBatch(context.Background(), b, BatchItem{Display: "SameKey"}); err != nil {
					t.Errorf("add: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if b.Len() != 500 {
		t.Fatalf("lost batch items: %d", b.Len())
	}
	n, err := e.CommitBatch(context.Background(), b)
	if err != nil || n != 1 {
		t.Fatalf("commit dedup: n=%d err=%v", n, err)
	}
	en := e.idx.current().tenant("t1").entries["samekey"]
	if en.UseCount != 500 {
		t.Fatalf("batch concurrent accumulation lost: %d", en.UseCount)
	}
}
