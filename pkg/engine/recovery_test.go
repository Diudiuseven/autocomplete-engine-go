package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Diudiuseven/autocomplete-engine-go/pkg/tenant"
)

func newPersistentEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.QueryTimeout = 2 * time.Second
	eng, err := New(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := eng.CreateTenant("t1", tenant.Quota{}); err != nil && err != ErrTenantExists {
		t.Fatal(err)
	}
	return eng
}

// TestRecoveryCommittedOnly: committed data survives restart; the WAL is
// replayed exactly once (no double-apply).
func TestRecoveryCommittedOnly(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	eng := newPersistentEngine(t, dir)
	if err := eng.Upsert(ctx, "t1", "alpha", 2, 2, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Upsert(ctx, "t1", "beta", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	eng2 := newPersistentEngine(t, dir)
	defer eng2.Close()
	// data present
	c, err := eng2.Lookup("t1", "alpha")
	if err != nil {
		t.Fatalf("alpha lost after restart: %v", err)
	}
	if c.Heat != 2 || c.Uses != 2 {
		t.Fatalf("alpha: %v/%v, want 2/2", c.Heat, c.Uses)
	}
	// replay must not double-apply: beta still 1/1
	c, _ = eng2.Lookup("t1", "beta")
	if c.Heat != 1 || c.Uses != 1 {
		t.Fatalf("beta double-applied: %v/%v", c.Heat, c.Uses)
	}
}

// TestRecoveryUncommittedIgnored: a transaction without a commit marker
// (simulated crash mid-commit) must not appear after restart.
func TestRecoveryUncommittedIgnored(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	eng := newPersistentEngine(t, dir)
	if err := eng.Upsert(ctx, "t1", "committed", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crashed transaction: append begin+put but no commit.
	walPath := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("00000000 {\"txid\":99,\"op\":\"begin\"}\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("00000000 {\"txid\":99,\"op\":\"put\",\"tenant\":\"t1\",\"term\":\"ghost\",\"heat\":1,\"uses\":1}\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	eng2 := newPersistentEngine(t, dir)
	defer eng2.Close()
	if _, err := eng2.Lookup("t1", "ghost"); err != ErrTermNotFound {
		t.Fatalf("uncommitted ghost appeared after restart: %v", err)
	}
	if _, err := eng2.Lookup("t1", "committed"); err != nil {
		t.Fatalf("committed data lost: %v", err)
	}
}

// TestRecoveryTornTail: a CRC-broken final line is ignored (torn write).
func TestRecoveryTornTail(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	eng := newPersistentEngine(t, dir)
	if err := eng.Upsert(ctx, "t1", "ok", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("deadbeef {not valid json\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	eng2 := newPersistentEngine(t, dir)
	defer eng2.Close()
	if _, err := eng2.Lookup("t1", "ok"); err != nil {
		t.Fatalf("committed data lost after torn tail: %v", err)
	}
}

// TestRecoveryCheckpoint: after a checkpoint, restart still yields all data
// and the WAL is truncated.
func TestRecoveryCheckpoint(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	eng := newPersistentEngine(t, dir)
	for i := 0; i < 20; i++ {
		if err := eng.Upsert(ctx, "t1", "term", 1, 1, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	// wal.log should be truncated after checkpoint
	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("wal.log not truncated after checkpoint: %d bytes", info.Size())
	}

	eng2 := newPersistentEngine(t, dir)
	defer eng2.Close()
	c, err := eng2.Lookup("t1", "term")
	if err != nil {
		t.Fatalf("data lost after checkpoint restart: %v", err)
	}
	if c.Heat != 20 || c.Uses != 20 {
		t.Fatalf("checkpoint data: %v/%v, want 20/20", c.Heat, c.Uses)
	}
}

// TestRecoveryDelete: deletes survive restart.
func TestRecoveryDelete(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	eng := newPersistentEngine(t, dir)
	if err := eng.Upsert(ctx, "t1", "a", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Upsert(ctx, "t1", "b", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Delete(ctx, "t1", "a"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	eng2 := newPersistentEngine(t, dir)
	defer eng2.Close()
	if _, err := eng2.Lookup("t1", "a"); err != ErrTermNotFound {
		t.Fatalf("deleted term reappeared: %v", err)
	}
	if _, err := eng2.Lookup("t1", "b"); err != nil {
		t.Fatalf("b lost: %v", err)
	}
}

// TestRecoveryCheckpointThenWrites: after a checkpoint, new writes must get
// txIDs above the checkpoint marker so a crash before the next checkpoint
// replays them (regression: txn counter must not fall back below baseTxID).
func TestRecoveryCheckpointThenWrites(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	eng := newPersistentEngine(t, dir)
	if err := eng.Upsert(ctx, "t1", "old", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen, write more (no checkpoint this time), then crash (no Close)
	eng2 := newPersistentEngine(t, dir)
	if err := eng2.Upsert(ctx, "t1", "new", 1, 1, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// simulate crash: close the WAL file without checkpointing
	if err := eng2.wal.Close(); err != nil {
		t.Fatal(err)
	}

	eng3 := newPersistentEngine(t, dir)
	defer eng3.Close()
	if _, err := eng3.Lookup("t1", "new"); err != nil {
		t.Fatalf("post-checkpoint write lost after crash: %v", err)
	}
	if _, err := eng3.Lookup("t1", "old"); err != nil {
		t.Fatalf("pre-checkpoint data lost: %v", err)
	}
}
