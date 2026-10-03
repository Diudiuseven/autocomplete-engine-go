package autocomplete

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func persistConfig(dir string, every int) Config {
	c := testConfig()
	c.EnablePersistence = true
	c.WALDir = dir
	c.SnapshotEveryN = every
	c.CheckpointMaxAge = 0 // keep checkpoint timing explicit in tests
	return c
}

func countEntries(e *Engine, tenant string) int {
	return len(e.idx.current().tenant(tenant).entries)
}

func TestRecoveryCommittedState(t *testing.T) {
	dir := t.TempDir()
	cfg := persistConfig(dir, 0)
	e, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := e.RegisterTenant(ctx, "t1", 0); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "Apple", Heat: 3, UseCount: 2, LastUsedAt: now})
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "apple", Heat: 1, UseCount: 3, LastUsedAt: now})
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "Banana", Heat: 1, UseCount: 1, LastUsedAt: now})
	_ = e.Delete(ctx, "t1", "Banana")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	if got := countEntries(e2, "t1"); got != 1 {
		t.Fatalf("expected 1 entry after recovery (dedup+delete), got %d", got)
	}
	en := e2.idx.current().tenant("t1").entries["apple"]
	if en.Heat != 4 || en.UseCount != 5 {
		t.Fatalf("accumulated heat/uses must survive restart: %+v", en)
	}
}

func TestRecoveryUnfinishedBatchDiscarded(t *testing.T) {
	dir := t.TempDir()
	cfg := persistConfig(dir, 0)
	e, _ := NewEngine(cfg)
	ctx := context.Background()
	_ = e.RegisterTenant(ctx, "t1", 0)
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "committed"})

	// Simulate a crash after begin+rows but before commit: write rows only.
	b, _ := e.BeginBatch(ctx, "t1")
	_ = e.AddToBatch(ctx, b, BatchItem{Display: "partial1"})
	_ = e.AddToBatch(ctx, b, BatchItem{Display: "partial2"})
	if err := e.persist.log(walRecord{Op: opBatchBegin, Tenant: "t1", BatchID: b.id}, false); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"partial1", "partial2"} {
		en, _ := e.prepareEntry("t1", BatchItem{Display: d})
		if err := e.persist.log(entryToRecord("t1", b.id, en), false); err != nil {
			t.Fatal(err)
		}
	}
	_ = e.Close()

	e2, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("reopen after torn batch: %v", err)
	}
	defer e2.Close()
	if got := countEntries(e2, "t1"); got != 1 {
		t.Fatalf("uncommitted batch rows must be dropped, got %d", got)
	}
	if _, ok := e2.idx.current().tenant("t1").entries["committed"]; !ok {
		t.Fatal("committed data must survive")
	}
}

func TestRecoveryAfterCheckpointNoReapply(t *testing.T) {
	dir := t.TempDir()
	cfg := persistConfig(dir, 3) // checkpoint every 3 records
	e, _ := NewEngine(cfg)
	ctx := context.Background()
	_ = e.RegisterTenant(ctx, "t1", 0) // record 1 -> checkpoint after 3rd
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "a"})
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "b"})
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "c"})
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "c", Heat: 4, UseCount: 4})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, snapshotFileName)); err != nil {
		t.Fatalf("snapshot expected: %v", err)
	}

	e2, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	if got := countEntries(e2, "t1"); got != 3 {
		t.Fatalf("snapshot replay produced %d entries, want 3", got)
	}
	en := e2.idx.current().tenant("t1").entries["c"]
	// first c contributes default heat 1/use 1, second contributes 4/4
	if en.Heat != 5 || en.UseCount != 5 {
		t.Fatalf("post-checkpoint accumulation wrong: %+v", en)
	}
}

func TestRecoveryTornTrailingFrame(t *testing.T) {
	dir := t.TempDir()
	cfg := persistConfig(dir, 0)
	e, _ := NewEngine(cfg)
	ctx := context.Background()
	_ = e.RegisterTenant(ctx, "t1", 0)
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "good"})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	// Append garbage bytes simulating a torn write at the tail.
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{walFrameMagic, 9, 0, 0, 0, 'a', 'b'}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	e2, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("recovery must tolerate torn tail: %v", err)
	}
	defer e2.Close()
	if got := countEntries(e2, "t1"); got != 1 {
		t.Fatalf("committed entry expected, got %d", got)
	}
}

func TestBatchCommitPersists(t *testing.T) {
	dir := t.TempDir()
	cfg := persistConfig(dir, 0)
	e, _ := NewEngine(cfg)
	ctx := context.Background()
	_ = e.RegisterTenant(ctx, "t1", 0)
	src := NewMemorySource([]BatchItem{{Display: "one"}, {Display: "two"}})
	if _, _, err := e.Import(ctx, "t1", src); err != nil {
		t.Fatal(err)
	}
	_ = e.Close()

	e2, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if got := countEntries(e2, "t1"); got != 2 {
		t.Fatalf("batch should survive restart as one committed unit, got %d", got)
	}
}

func TestSnapshotCorruptRejected(t *testing.T) {
	dir := t.TempDir()
	cfg := persistConfig(dir, 1)
	e, _ := NewEngine(cfg)
	ctx := context.Background()
	_ = e.RegisterTenant(ctx, "t1", 0)
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "x"})
	_ = e.Close()
	if err := os.WriteFile(filepath.Join(dir, snapshotFileName), []byte("not a gob snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngine(cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt snapshot must be reported, got %v", err)
	}
}

type persistClock struct{ t time.Time }

func (c *persistClock) Now() time.Time { return c.t }

func TestCheckpointByAge(t *testing.T) {
	dir := t.TempDir()
	cfg := persistConfig(dir, 0)
	cfg.CheckpointMaxAge = time.Minute
	clk := &persistClock{t: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	e, err := NewEngineWithClock(cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = e.RegisterTenant(ctx, "t1", 0)
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "a"})
	if _, err := os.Stat(filepath.Join(dir, snapshotFileName)); !os.IsNotExist(err) {
		t.Fatalf("no snapshot expected yet, err=%v", err)
	}
	clk.t = clk.t.Add(2 * time.Minute)
	_ = e.Upsert(ctx, "t1", BatchItem{Display: "b"})
	if _, err := os.Stat(filepath.Join(dir, snapshotFileName)); err != nil {
		t.Fatalf("age-based snapshot expected: %v", err)
	}
	_ = e.Close()

	e2, err := NewEngineWithClock(cfg, clk)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if got := countEntries(e2, "t1"); got != 2 {
		t.Fatalf("entries after age checkpoint recovery: %d", got)
	}
}
