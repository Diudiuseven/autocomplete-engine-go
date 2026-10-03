package autocomplete

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBatchCommitAtomicAndDedup(t *testing.T) {
	e, clk := newTestEngine(t, testConfig())
	now := clk.Now()
	b, err := e.BeginBatch(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	items := []BatchItem{
		{Display: "Alpha", Heat: 2, UseCount: 1, LastUsedAt: now},
		{Display: "alpha", Heat: 3, UseCount: 4, LastUsedAt: now}, // duplicate key
		{Display: "Beta", Heat: 1, UseCount: 1, LastUsedAt: now},
	}
	for _, it := range items {
		if err := e.AddToBatch(context.Background(), b, it); err != nil {
			t.Fatal(err)
		}
	}
	// Before commit nothing is visible.
	if got, _, _ := e.Query(context.Background(), "t1", "alpha", QueryOptions{}); len(got) != 0 {
		t.Fatalf("uncommitted batch visible: %v", keysOf(got))
	}
	n, err := e.CommitBatch(context.Background(), b)
	if err != nil || n != 2 {
		t.Fatalf("commit: n=%d err=%v", n, err)
	}
	ts := e.idx.current().tenant("t1")
	if len(ts.entries) != 2 {
		t.Fatalf("batch dedup expected 2 entries, got %d", len(ts.entries))
	}
	a := ts.entries["alpha"]
	if a.Heat != 5 || a.UseCount != 5 {
		t.Fatalf("batch accumulation lost: %+v", a)
	}
	// double commit rejected
	if _, err := e.CommitBatch(context.Background(), b); !errors.Is(err, ErrBatchConflict) {
		t.Fatalf("double commit: %v", err)
	}
}

func TestBatchAbortLeavesNothing(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	b, _ := e.BeginBatch(context.Background(), "t1")
	if err := e.AddToBatch(context.Background(), b, BatchItem{Display: "ghost"}); err != nil {
		t.Fatal(err)
	}
	if err := e.AbortBatch(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := e.AddToBatch(context.Background(), b, BatchItem{Display: "x"}); !errors.Is(err, ErrBatchConflict) {
		t.Fatalf("add after abort: %v", err)
	}
	if _, _, err := e.Query(context.Background(), "t1", "ghost", QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(e.idx.current().tenant("t1").entries) != 0 {
		t.Fatal("aborted batch must leave no entries")
	}
}

func TestBatchQuotaRollback(t *testing.T) {
	cfg := testConfig()
	e, _ := newTestEngine(t, cfg)
	if err := e.RegisterTenant(context.Background(), "q", 1); err != nil {
		t.Fatal(err)
	}
	upsert(t, e, "q", "existing", 1, 1, time.Time{})
	b, _ := e.BeginBatch(context.Background(), "q")
	_ = e.AddToBatch(context.Background(), b, BatchItem{Display: "one"})
	_ = e.AddToBatch(context.Background(), b, BatchItem{Display: "two"})
	_, err := e.CommitBatch(context.Background(), b)
	if !errors.Is(err, ErrTenantQuotaExceeded) {
		t.Fatalf("quota: %v", err)
	}
	if len(e.idx.current().tenant("q").entries) != 1 {
		t.Fatalf("failed batch must not partially publish")
	}
}

func TestImportMemoryAndChannel(t *testing.T) {
	e, _ := newTestEngine(t, testConfig())
	src := NewMemorySource([]BatchItem{{Display: "Car"}, {Display: "CAR"}, {Display: "Truck"}})
	id, n, err := e.Import(context.Background(), "t1", src)
	if err != nil || n != 2 || id == "" {
		t.Fatalf("import: id=%q n=%d err=%v", id, n, err)
	}
	if len(e.idx.current().tenant("t1").entries) != 2 {
		t.Fatalf("import dedup failed")
	}

	ch := make(chan BatchItem, 2)
	ch <- BatchItem{Display: "bike"}
	ch <- BatchItem{Display: "BIKE"}
	close(ch)
	_, n, err = e.Import(context.Background(), "t1", NewChannelSource(ch))
	if err != nil || n != 1 {
		t.Fatalf("channel import: n=%d err=%v", n, err)
	}
}

func TestFileSourceImport(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "terms.tsv")
	content := "# comment\n\napple\t5\t2\t2025-01-02T03:04:05Z\nBANANA\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileSource(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	e, _ := newTestEngine(t, testConfig())
	id, n, err := e.Import(context.Background(), "t1", fs)
	if err != nil || n != 2 || id == "" {
		t.Fatalf("file import: n=%d err=%v", n, err)
	}
	en := e.idx.current().tenant("t1").entries["apple"]
	if en.Heat != 5 || en.UseCount != 2 || en.LastUsedAt.IsZero() {
		t.Fatalf("parsed fields wrong: %+v", en)
	}
}

func TestFileSourceBadLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.tsv")
	if err := os.WriteFile(path, []byte("ok\tx\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fs, err := NewFileSource(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Next(); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("bad heat line: %v", err)
	}
}
