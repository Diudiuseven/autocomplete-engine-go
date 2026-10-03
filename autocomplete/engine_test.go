package autocomplete

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func testConfig() Config {
	c := DefaultConfig()
	c.EnablePersistence = false
	c.QueryTimeout = 200 * time.Millisecond
	return c
}

func newTestEngine(t *testing.T, cfg Config) (*Engine, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)}
	e, err := NewEngineWithClock(cfg, clk)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	if err := e.RegisterTenant(context.Background(), "t1", 0); err != nil {
		t.Fatalf("register: %v", err)
	}
	return e, clk
}

func upsert(t *testing.T, e *Engine, tenant, display string, heat float64, uses int64, at time.Time) {
	t.Helper()
	if err := e.Upsert(context.Background(), tenant, BatchItem{Display: display, Heat: heat, UseCount: uses, LastUsedAt: at}); err != nil {
		t.Fatalf("upsert %q: %v", display, err)
	}
}

func keysOf(cs []Candidate) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Entry.Key
	}
	return out
}

func TestInvalidAndTooLongInput(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	ctx := context.Background()
	if err := e.Upsert(ctx, "t1", BatchItem{Display: "   "}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("blank: %v", err)
	}
	if err := e.Upsert(ctx, "t1", BatchItem{Display: "ok", Heat: -1, UseCount: 1}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("negative heat: %v", err)
	}
	if _, _, err := e.Query(ctx, "t1", "", QueryOptions{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty query: %v", err)
	}
	long := make([]byte, testConfig().MaxInputRunes+10)
	for i := range long {
		long[i] = 'a'
	}
	if err := e.Upsert(ctx, "t1", BatchItem{Display: string(long)}); !errors.Is(err, ErrTooLong) {
		t.Fatalf("too long: %v", err)
	}
	if err := e.RegisterTenant(ctx, "", 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("empty tenant: %v", err)
	}
}

func TestPrefixInfixAndDisplayPreserved(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	upsert(t, e, "t1", "Apple", 10, 5, now)
	upsert(t, e, "t1", "Pineapple", 10, 5, now)
	upsert(t, e, "t1", "Banana", 10, 5, now)

	got, _, err := e.Query(context.Background(), "t1", "app", QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 matches (prefix+infix), got %d: %v", len(got), keysOf(got))
	}
	if got[0].Match != MatchPrefix || got[0].Entry.Display != "Apple" {
		t.Fatalf("first must be prefix Apple, got %+v", got[0])
	}
	if got[1].Match != MatchInfix || got[1].Entry.Display != "Pineapple" {
		t.Fatalf("second must be infix Pineapple, got %+v", got[1])
	}
	// Case-insensitive but original form retained.
	got2, _, err := e.Query(context.Background(), "t1", "APPLE", QueryOptions{})
	if err != nil || len(got2) != 2 {
		t.Fatalf("case-insensitive: %v %v", got2, err)
	}
	for _, c := range got2 {
		if c.Entry.Display == c.Entry.Key {
			t.Fatalf("display must retain original form: %+v", c.Entry)
		}
	}
}

func TestDedupAccumulatesNotOverwrites(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	upsert(t, e, "t1", "Café", 3, 2, now)
	upsert(t, e, "t1", "café", 4, 7, now) // same normalized key
	ts := e.idx.current().tenant("t1")
	en := ts.entries["café"]
	if en.Heat != 7 || en.UseCount != 9 {
		t.Fatalf("heat/uses not accumulated: %+v", en)
	}
	if en.Display != "café" {
		t.Fatalf("display should follow latest update: %q", en.Display)
	}
	if len(ts.entries) != 1 {
		t.Fatalf("duplicate must keep one entry, got %d", len(ts.entries))
	}
}

func TestDeterministicOrderRepeatedQueries(t *testing.T) {
	cfg := testConfig()
	e, clk := newTestEngine(t, cfg)
	t0 := clk.Now()
	// Equal heat/uses/time; tie-break must be deterministic on key.
	for _, w := range []string{"kiwi", "apple", "mango"} {
		upsert(t, e, "t1", w, 1, 1, t0)
	}
	got, _, err := e.Query(context.Background(), "t1", "a", QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Entry.Key != "apple" || got[1].Entry.Key != "mango" {
		t.Fatalf("deterministic key tiebreak failed: %v", keysOf(got))
	}
	// New engine, same data, same logical time -> same order.
	e2, clk2 := newTestEngine(t, cfg)
	for _, w := range []string{"kiwi", "apple", "mango"} {
		upsert(t, e2, "t1", w, 1, 1, clk2.Now())
	}
	got2, _, err := e2.Query(context.Background(), "t1", "a", QueryOptions{})
	if err != nil || fmt.Sprint(keysOf(got2)) != fmt.Sprint(keysOf(got)) {
		t.Fatalf("cross-engine order mismatch: %v vs %v (%v)", keysOf(got2), keysOf(got), err)
	}
}

func TestHeatDecayAndFreshnessRanking(t *testing.T) {
	cfg := testConfig()
	cfg.HalfLife = 24 * time.Hour
	cfg.HeatWeight = 1
	cfg.FreshnessWeight = 1
	cfg.FreshnessWindow = 60 * 24 * time.Hour
	e, clk := newTestEngine(t, cfg)
	t0 := clk.Now()
	// At t0: old has 10 heat used 2 days ago; new has 1 heat used now.
	upsert(t, e, "t1", "oldterm", 10, 10, t0.Add(-2*24*time.Hour))
	upsert(t, e, "t1", "newterm", 1, 1, t0)
	got, _, err := e.Query(context.Background(), "t1", "term", QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Entry.Key != "oldterm" {
		t.Fatalf("recently strong old term expected first, got %v", keysOf(got))
	}
	// Advance 30 days: old heat decays below the fresh-but-small entry.
	clk.advance(30 * 24 * time.Hour)
	got2, _, err := e.Query(context.Background(), "t1", "term", QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got2[0].Entry.Key != "newterm" {
		t.Fatalf("after decay fresh term expected first, got %v", keysOf(got2))
	}
	// Another 60 days: both equally stale (freshness 0), use count decides.
	clk.advance(60 * 24 * time.Hour)
	got3, _, _ := e.Query(context.Background(), "t1", "term", QueryOptions{})
	if got3[0].Entry.Key != "oldterm" {
		t.Fatalf("fully stale tie broken by use count, got %v", keysOf(got3))
	}
}

func TestFreshnessOnlyConfig(t *testing.T) {
	cfg := testConfig()
	cfg.HeatWeight = 0
	cfg.FreshnessWeight = 1
	e, clk := newTestEngine(t, cfg)
	upsert(t, e, "t1", "oldterm", 1000, 10, clk.Now().Add(-30*24*time.Hour))
	upsert(t, e, "t1", "newterm", 1, 1, clk.Now())
	got, _, err := e.Query(context.Background(), "t1", "term", QueryOptions{})
	if err != nil || got[0].Entry.Key != "newterm" {
		t.Fatalf("freshness-only ranking failed: %v %v", keysOf(got), err)
	}
}

func TestTenantIsolationAndReject(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	if err := e.RegisterTenant(context.Background(), "t2", 0); err != nil {
		t.Fatal(err)
	}
	upsert(t, e, "t1", "secret", 1, 1, now)
	if _, _, err := e.Query(context.Background(), "t2", "secret", QueryOptions{}); !errors.Is(err, ErrUnknownTenant) || true {
		// t2 exists, so query succeeds with zero results
	}
	got, _, err := e.Query(context.Background(), "t2", "secret", QueryOptions{})
	if err != nil {
		t.Fatalf("query known tenant: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tenant leak: t2 saw t1 candidate %v", keysOf(got))
	}
	if _, _, err := e.Query(context.Background(), "ghost", "x", QueryOptions{}); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("cross/unknown tenant must be rejected: %v", err)
	}
	if err := e.Upsert(context.Background(), "ghost", BatchItem{Display: "x"}); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("write to unknown tenant must be rejected: %v", err)
	}
}

func TestQuota(t *testing.T) {
	cfg := testConfig()
	e, _ := newTestEngine(t, cfg)
	if err := e.RegisterTenant(context.Background(), "q", 2); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := e.Upsert(context.Background(), "q", BatchItem{Display: "a", LastUsedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := e.Upsert(context.Background(), "q", BatchItem{Display: "b", LastUsedAt: now}); err != nil {
		t.Fatal(err)
	}
	// updating existing key is allowed despite quota
	if err := e.Upsert(context.Background(), "q", BatchItem{Display: "a", LastUsedAt: now}); err != nil {
		t.Fatalf("update within quota: %v", err)
	}
	if err := e.Upsert(context.Background(), "q", BatchItem{Display: "c", LastUsedAt: now}); !errors.Is(err, ErrTenantQuotaExceeded) {
		t.Fatalf("quota: %v", err)
	}
	if got := e.idx.current().tenant("q").stats.Candidates; got != 2 {
		t.Fatalf("failed upsert must not change size: %d", got)
	}
}

func TestConcurrentUpdatesNoLoss(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	now := time.Now().UTC()
	const goroutines, per = 20, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := e.Upsert(context.Background(), "t1", BatchItem{Display: "hot", Heat: 1, UseCount: 1, LastUsedAt: now}); err != nil {
					t.Errorf("upsert: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	en := e.idx.current().tenant("t1").entries["hot"]
	if en.UseCount != goroutines*per || en.Heat != float64(goroutines*per) {
		t.Fatalf("lost accumulation: heat=%v uses=%d want %d", en.Heat, en.UseCount, goroutines*per)
	}
	if len(e.idx.current().tenant("t1").entries) != 1 {
		t.Fatalf("must remain one entry")
	}
}
