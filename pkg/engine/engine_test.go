package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/autocomplete-engine-go/pkg/clock"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/tenant"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = ""
	cfg.QueryTimeout = 2 * time.Second
	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	if err := eng.CreateTenant("t1", tenant.Quota{}); err != nil {
		t.Fatal(err)
	}
	return eng
}

func TestSearchPrefixAndSubstring(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	for _, term := range []string{"apple", "application", "pineapple", "banana"} {
		if err := eng.Upsert(ctx, "t1", term, 1, 1, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	// prefix match ("app" is also a substring of "pineapple")
	res, err := eng.Search(ctx, "t1", "app", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Matches) != 3 {
		t.Fatalf("prefix: got %d matches, want 3: %+v", len(res.Matches), res.Matches)
	}
	// substring match
	res, err = eng.Search(ctx, "t1", "apple", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Matches) != 2 {
		t.Fatalf("substring: got %d matches, want 2 (apple, pineapple)", len(res.Matches))
	}
	// no match
	res, err = eng.Search(ctx, "t1", "zzz", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Matches) != 0 {
		t.Fatalf("no-match: got %d", len(res.Matches))
	}
}

func TestSearchCaseInsensitiveDisplayPreserved(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	if err := eng.Upsert(ctx, "t1", "Apple", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	res, err := eng.Search(ctx, "t1", "APP", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Matches) != 1 || res.Matches[0].Term != "Apple" {
		t.Fatalf("got %+v, want [Apple]", res.Matches)
	}
}

func TestSearchDeterministicOrder(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	now := time.Now()
	terms := []string{"banana", "apple", "apricot", "avocado"}
	for i, term := range terms {
		if err := eng.Upsert(ctx, "t1", term, float64(i+1), int64(i+1), now); err != nil {
			t.Fatal(err)
		}
	}
	first, err := eng.Search(ctx, "t1", "a", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		next, err := eng.Search(ctx, "t1", "a", SearchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(next.Matches) != len(first.Matches) {
			t.Fatal("match count changed")
		}
		for j := range first.Matches {
			if next.Matches[j].Term != first.Matches[j].Term {
				t.Fatalf("order changed: %+v vs %+v", first.Matches, next.Matches)
			}
		}
	}
	// higher heat ranks first
	if first.Matches[0].Term != "avocado" {
		t.Fatalf("want avocado (heat 4) first, got %s", first.Matches[0].Term)
	}
}

func TestRepeatedUpdatesAccumulate(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := eng.Upsert(ctx, "t1", "coffee", 2, 3, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	c, err := eng.Lookup("t1", "coffee")
	if err != nil {
		t.Fatal(err)
	}
	if c.Heat != 10 || c.Uses != 15 {
		t.Fatalf("accumulation: heat=%v uses=%v, want 10/15", c.Heat, c.Uses)
	}
}

func TestDelete(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	if err := eng.Upsert(ctx, "t1", "tea", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Delete(ctx, "t1", "tea"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Lookup("t1", "tea"); err != ErrTermNotFound {
		t.Fatalf("want ErrTermNotFound, got %v", err)
	}
	if err := eng.Delete(ctx, "t1", "tea"); err != ErrTermNotFound {
		t.Fatalf("second delete: want ErrTermNotFound, got %v", err)
	}
}

func TestTenantIsolation(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	if err := eng.CreateTenant("t2", tenant.Quota{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Upsert(ctx, "t1", "secret", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// t2 must not see t1's candidate
	res, err := eng.Search(ctx, "t2", "secret", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Matches) != 0 {
		t.Fatalf("tenant leak: %+v", res.Matches)
	}
	// cross-tenant write path does not exist; unknown tenant is rejected
	if err := eng.Upsert(ctx, "nope", "x", 1, 1, time.Time{}); err != ErrTenantNotFound {
		t.Fatalf("want ErrTenantNotFound, got %v", err)
	}
	if err := eng.Delete(ctx, "nope", "x"); err != ErrTenantNotFound {
		t.Fatalf("want ErrTenantNotFound, got %v", err)
	}
	if _, err := eng.Search(ctx, "nope", "x", SearchOptions{}); err != ErrTenantNotFound {
		t.Fatalf("want ErrTenantNotFound, got %v", err)
	}
	// stats are per-tenant
	st1, _ := eng.Stats("t1")
	st2, _ := eng.Stats("t2")
	if st1.Candidates != 1 || st2.Candidates != 0 {
		t.Fatalf("stats leak: %+v %+v", st1, st2)
	}
}

func TestQuota(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	if err := eng.CreateTenant("q", tenant.Quota{MaxCandidates: 2}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Upsert(ctx, "q", "a", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Upsert(ctx, "q", "b", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Upsert(ctx, "q", "c", 1, 1, time.Time{}); err != ErrQuotaExceeded {
		t.Fatalf("want ErrQuotaExceeded, got %v", err)
	}
	// failed write must not pollute the committed version
	if _, err := eng.Lookup("q", "c"); err != ErrTermNotFound {
		t.Fatalf("quota failure polluted state")
	}
	st, _ := eng.Stats("q")
	if st.Candidates != 2 {
		t.Fatalf("quota stats: %d", st.Candidates)
	}
}

func TestInputValidation(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	long := make([]byte, 100)
	for i := range long {
		long[i] = 'a'
	}
	if err := eng.Upsert(ctx, "t1", "", 1, 1, time.Time{}); err != ErrEmptyTerm {
		t.Fatalf("empty term: %v", err)
	}
	if err := eng.Upsert(ctx, "t1", string(long), 1, 1, time.Time{}); err != ErrTermTooLong {
		t.Fatalf("long term: %v", err)
	}
	if err := eng.Upsert(ctx, "t1", "bad\x00", 1, 1, time.Time{}); err != ErrInvalidTerm {
		t.Fatalf("invalid term: %v", err)
	}
	if err := eng.Upsert(ctx, "t1", "ok", -1, 1, time.Time{}); err != ErrNegativeDelta {
		t.Fatalf("negative heat: %v", err)
	}
	if _, err := eng.Search(ctx, "t1", "", SearchOptions{}); err != ErrEmptyQuery {
		t.Fatalf("empty query: %v", err)
	}
	if _, err := eng.Search(ctx, "t1", string(long), SearchOptions{}); err != ErrQueryTooLong {
		t.Fatalf("long query: %v", err)
	}
}

func TestDecayUsesInjectedClock(t *testing.T) {
	fc := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	cfg := DefaultConfig()
	cfg.DataDir = ""
	cfg.Clock = fc
	cfg.HeatHalfLife = 10 * 24 * time.Hour
	cfg.RecencyHalfLife = 10 * 24 * time.Hour
	eng, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateTenant("t1", tenant.Quota{}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := eng.Upsert(ctx, "t1", "cold", 100, 1, fc.Now()); err != nil {
		t.Fatal(err)
	}
	if err := eng.Upsert(ctx, "t1", "old", 100, 1, fc.Now().Add(-10*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	res, err := eng.Search(ctx, "t1", "old", SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Matches) != 2 {
		t.Fatalf("want 2 matches, got %d", len(res.Matches))
	}
	// cold (age 0, decay 1) beats old (age 10d, decay 0.5)
	if res.Matches[0].Term != "cold" {
		t.Fatalf("want cold first, got %s (score %v vs %v)", res.Matches[0].Term, res.Matches[0].Score, res.Matches[1].Score)
	}
	// after one half-life the old term's heat component is halved
	fc.Advance(10 * 24 * time.Hour)
	res, _ = eng.Search(ctx, "t1", "old", SearchOptions{})
	if res.Matches[0].Term != "cold" {
		t.Fatalf("want cold still first after decay")
	}
}
