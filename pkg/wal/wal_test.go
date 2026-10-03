package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReplayCommittedOnly(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// committed txn 1
	if err := w.Begin(1); err != nil {
		t.Fatal(err)
	}
	if err := w.Put(Record{TxID: 1, Op: OpPut, Tenant: "t", Term: "a", Heat: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(1); err != nil {
		t.Fatal(err)
	}
	// uncommitted txn 2 (no commit)
	if err := w.Begin(2); err != nil {
		t.Fatal(err)
	}
	if err := w.Put(Record{TxID: 2, Op: OpPut, Tenant: "t", Term: "ghost", Heat: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	var got []Record
	_, _, err = w2.Replay(0, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Term != "a" {
		t.Fatalf("replay got %+v, want only committed 'a'", got)
	}
}

func TestReplayAfterTxID(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	for _, id := range []int64{1, 2, 3} {
		w.Begin(id)
		w.Put(Record{TxID: id, Op: OpPut, Tenant: "t", Term: "v"})
		w.Commit(id)
	}
	w.Close()

	w2, _ := Open(dir)
	defer w2.Close()
	var n int
	committed, _, err := w2.Replay(2, func(r Record) error {
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("replayed %d records after txid 2, want 1", n)
	}
	if committed != 3 {
		t.Fatalf("committed=%d, want 3", committed)
	}
}

func TestCheckpointTruncates(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.Begin(1)
	w.Put(Record{TxID: 1, Op: OpPut, Tenant: "t", Term: "a"})
	w.Commit(1)
	if err := w.Checkpoint(1, []byte(`{"snapshot":true}`)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("wal not truncated: %d bytes", info.Size())
	}
	b, err := w.LoadSnapshot()
	if err != nil || string(b) != `{"snapshot":true}` {
		t.Fatalf("snapshot: %q %v", b, err)
	}
	txID, err := w.CheckpointTxID()
	if err != nil || txID != 1 {
		t.Fatalf("checkpoint txid: %d %v", txID, err)
	}
	w.Close()
}

func TestTornLineStopsReplay(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.Begin(1)
	w.Put(Record{TxID: 1, Op: OpPut, Tenant: "t", Term: "a"})
	w.Commit(1)
	w.Close()
	// append a corrupt line (bad CRC)
	f, _ := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("ffffffff {\"txid\":2,\"op\":\"begin\"}\n")
	f.Close()

	w2, _ := Open(dir)
	defer w2.Close()
	var n int
	_, _, err := w2.Replay(0, func(r Record) error { n++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("torn line: replayed %d, want 1 (corrupt tail must be ignored)", n)
	}
}
