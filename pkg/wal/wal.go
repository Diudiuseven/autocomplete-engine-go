// Package wal implements a write-ahead log with committed/uncommitted
// transaction markers and CRC-protected records, plus checkpointing.
//
// Recovery rule: only records belonging to transactions that carry a commit
// marker are replayed; torn records (CRC mismatch) and transactions without
// a commit marker are ignored. Checkpoints persist a full snapshot atomically
// (temp file + rename + fsync) and truncate the log, so recovery never has to
// guess whether data was applied.
package wal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Op identifies a log record.
type Op string

const (
	OpBegin  Op = "begin"
	OpPut    Op = "put"
	OpDelete Op = "delete"
	OpCommit Op = "commit"
)

// Record is one WAL entry.
type Record struct {
	TxID   int64   `json:"txid"`
	Op     Op      `json:"op"`
	Tenant string  `json:"tenant,omitempty"`
	Term   string  `json:"term,omitempty"`
	Heat   float64 `json:"heat,omitempty"`
	Uses   int64   `json:"uses,omitempty"`
	When   int64   `json:"when,omitempty"` // unix nano
}

const (
	logName        = "wal.log"
	snapshotName   = "snapshot.json"
	checkpointName = "checkpoint.json"
)

// WAL is an append-only log.
type WAL struct {
	mu  sync.Mutex
	dir string
	f   *os.File
}

// Open opens (or creates) the WAL in dir.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, logName), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	return &WAL{dir: dir, f: f}, nil
}

func (w *WAL) write(r Record, sync bool) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	line := fmt.Sprintf("%08x %s\n", crc32.ChecksumIEEE(b), b)
	if _, err := w.f.WriteString(line); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	if sync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("wal: sync: %w", err)
		}
	}
	return nil
}

// Begin writes a begin marker for txID.
func (w *WAL) Begin(txID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.write(Record{TxID: txID, Op: OpBegin}, false)
}

// Put writes a put record.
func (w *WAL) Put(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	r.Op = OpPut
	return w.write(r, false)
}

// Delete writes a delete record.
func (w *WAL) Delete(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	r.Op = OpDelete
	return w.write(r, false)
}

// Commit writes the commit marker and forces it to disk.
func (w *WAL) Commit(txID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.write(Record{TxID: txID, Op: OpCommit}, true); err != nil {
		return err
	}
	return nil
}

// checkpointMarker is the durable checkpoint pointer.
type checkpointMarker struct {
	TxID int64 `json:"txid"`
}

// CheckpointTxID returns the txID covered by the last checkpoint (0 if none).
func (w *WAL) CheckpointTxID() (int64, error) {
	b, err := os.ReadFile(filepath.Join(w.dir, checkpointName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var m checkpointMarker
	if err := json.Unmarshal(b, &m); err != nil {
		return 0, fmt.Errorf("wal: bad checkpoint marker: %w", err)
	}
	return m.TxID, nil
}

// LoadSnapshot returns the last checkpoint snapshot bytes (nil if none).
func (w *WAL) LoadSnapshot() ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(w.dir, snapshotName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// Checkpoint atomically persists a full snapshot and truncates the log.
// Order: snapshot file -> marker -> log truncation. A crash at any point
// leaves a recoverable state (marker is the authority; replay filters txIDs
// <= marker).
func (w *WAL) Checkpoint(txID int64, snapshot []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. snapshot file (tmp + rename + fsync)
	tmp := filepath.Join(w.dir, snapshotName+".tmp")
	if err := os.WriteFile(tmp, snapshot, 0o644); err != nil {
		return fmt.Errorf("wal: snapshot write: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, snapshotName)); err != nil {
		return fmt.Errorf("wal: snapshot rename: %w", err)
	}
	if d, err := os.Open(w.dir); err == nil {
		_ = d.Sync()
		d.Close()
	}

	// 2. marker
	mb, _ := json.Marshal(checkpointMarker{TxID: txID})
	tmpM := filepath.Join(w.dir, checkpointName+".tmp")
	if err := os.WriteFile(tmpM, mb, 0o644); err != nil {
		return fmt.Errorf("wal: marker write: %w", err)
	}
	if err := os.Rename(tmpM, filepath.Join(w.dir, checkpointName)); err != nil {
		return fmt.Errorf("wal: marker rename: %w", err)
	}
	if d, err := os.Open(w.dir); err == nil {
		_ = d.Sync()
		d.Close()
	}

	// 3. truncate the log: close, truncate, reopen in append mode.
	if err := w.f.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(w.dir, logName), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: truncate reopen: %w", err)
	}
	w.f = f
	return nil
}

// Replay scans the log and invokes apply for every record of committed
// transactions with txID > afterTxID, in log order. It returns the largest
// committed txID and the largest txID seen overall (for txn counter reuse).
// A CRC mismatch or malformed line is treated as a torn tail and stops replay.
func (w *WAL) Replay(afterTxID int64, apply func(Record) error) (committedTxID, maxSeenTxID int64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := os.Open(filepath.Join(w.dir, logName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	pending := map[int64][]Record{} // txID -> records awaiting commit
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		rec, ok := parseLine(line)
		if !ok {
			break // torn tail: ignore the rest
		}
		if rec.TxID > maxSeenTxID {
			maxSeenTxID = rec.TxID
		}
		switch rec.Op {
		case OpBegin:
			pending[rec.TxID] = nil
		case OpPut, OpDelete:
			pending[rec.TxID] = append(pending[rec.TxID], rec)
		case OpCommit:
			if rec.TxID > afterTxID {
				for _, r := range pending[rec.TxID] {
					if err := apply(r); err != nil {
						return committedTxID, maxSeenTxID, err
					}
				}
				if rec.TxID > committedTxID {
					committedTxID = rec.TxID
				}
			}
			delete(pending, rec.TxID)
		}
	}
	if err := sc.Err(); err != nil {
		return committedTxID, maxSeenTxID, err
	}
	return committedTxID, maxSeenTxID, nil
}

// parseLine verifies the CRC and decodes one log line.
func parseLine(line []byte) (Record, bool) {
	sp := strings.IndexByte(string(line), ' ')
	if sp < 0 {
		return Record{}, false
	}
	want, err := strconv.ParseUint(string(line[:sp]), 16, 32)
	if err != nil {
		return Record{}, false
	}
	body := line[sp+1:]
	if crc32.ChecksumIEEE(body) != uint32(want) {
		return Record{}, false
	}
	var r Record
	if err := json.Unmarshal(body, &r); err != nil {
		return Record{}, false
	}
	return r, true
}

// Close closes the log file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		err := w.f.Close()
		w.f = nil
		return err
	}
	return nil
}
