package autocomplete

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// opCode tags each WAL record.
type opCode uint8

const (
	opUpsert opCode = iota + 1
	opDelete
	opBatchBegin
	opBatchCommit
	opTenantRegister
	opCheckpoint
)

const (
	walFileName    = "wal.log"
	walFrameMagic  = 0x57 // 'W'
	walMaxFrameLen = 8 << 20
)

// walRecord is one persisted operation. A batch import is:
//
//	opBatchBegin{BatchID}
//	opUpsert{BatchID,...} * N	(visible to nothing until commit)
//	opBatchCommit{BatchID}
//
// On recovery all records after the last commit for an open batch are
// discarded, which is exactly how an unfinished import leaves no partial data.
type walRecord struct {
	Seq        uint64
	Op         opCode
	Tenant     string
	Key        string
	Display    string
	Heat       float64
	UseCount   int64
	CreatedAt  time.Time
	LastUsedAt time.Time
	BatchID    string
	Quota      int
}

// WAL is an append-only log. Every frame is independent and CRC-protected, so
// a torn append (power loss after a short write) is detected and treated as
// the log's end rather than as corrupt committed data.
type WAL struct {
	dir  string
	path string
	mu   sync.Mutex
	f    *os.File
	seq  uint64
}

func openWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("autocomplete: wal dir: %w", err)
	}
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("autocomplete: open wal: %w", err)
	}
	return &WAL{dir: dir, path: path, f: f}, nil
}

func encodeFrame(rec walRecord) ([]byte, error) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(rec); err != nil {
		return nil, err
	}
	if payload.Len() > walMaxFrameLen {
		return nil, fmt.Errorf("%w: frame too large", ErrCorrupt)
	}
	frame := make([]byte, 0, payload.Len()+10)
	header := []byte{walFrameMagic, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(header[1:], uint32(payload.Len()))
	frame = append(frame, header...)
	frame = append(frame, payload.Bytes()...)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(payload.Bytes()))
	frame = append(frame, crc[:]...)
	return frame, nil
}

// append encodes and writes one frame. When fsync is true the record is made
// durable before append returns; commit boundaries always use fsync.
func (w *WAL) append(rec walRecord, fsync bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if rec.Seq == 0 {
		w.seq++
		rec.Seq = w.seq
	}
	frame, err := encodeFrame(rec)
	if err != nil {
		return err
	}
	if w.f == nil {
		return ErrClosed
	}
	if _, err := w.f.Write(frame); err != nil {
		return fmt.Errorf("autocomplete: wal write: %w", err)
	}
	if fsync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("autocomplete: wal sync: %w", err)
		}
	}
	return nil
}

// truncate drops all WAL content (called after a durable snapshot rename).
// setHighWater moves the sequence counter to at least seq. Called after
// recovery and again after truncating the WAL for a snapshot, so freshly
// appended records never reuse a Seq already covered by a snapshot.
func (w *WAL) setHighWater(seq uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if seq > w.seq {
		w.seq = seq
	}
}

func (w *WAL) truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	// Keep advancing Seq monotonically even though the file is empty; the
	// snapshot stored the previous high-water value.
	return w.f.Sync()
}

func (w *WAL) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

// scanWAL visits every intact frame in order. It returns the number of applied
// records. A torn trailing frame (short read, bad length, CRC mismatch, gob
// error) terminates the scan with io.ErrUnexpectedEOF, but ONLY when the bad
// frame reaches the end of the data: structural garbage followed by intact
// bytes is a hard ErrCorrupt instead.
func scanWAL(r io.Reader, visit func(walRecord) error) (int, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	n := 0
	pos := 0
	for pos < len(data) {
		if pos+5 > len(data) {
			return n, io.ErrUnexpectedEOF // torn header at tail
		}
		if data[pos] != walFrameMagic {
			return n, fmt.Errorf("%w: bad frame magic at byte %d", ErrCorrupt, pos)
		}
		ln := binary.BigEndian.Uint32(data[pos+1 : pos+5])
		end := pos + 5 + int(ln) + 4
		if ln == 0 || ln > walMaxFrameLen {
			// Implausible length is a torn tail only if the frame runs to/past EOF.
			if end >= len(data) {
				return n, io.ErrUnexpectedEOF
			}
			return n, fmt.Errorf("%w: implausible frame length %d at byte %d", ErrCorrupt, ln, pos)
		}
		if end > len(data) {
			return n, io.ErrUnexpectedEOF // truncated payload/crc at tail
		}
		payload := data[pos+5 : pos+5+int(ln)]
		crcBytes := data[pos+5+int(ln) : end]
		if binary.BigEndian.Uint32(crcBytes) != crc32.ChecksumIEEE(payload) {
			return n, io.ErrUnexpectedEOF // bad checksum => torn/partial write at tail
		}
		var rec walRecord
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&rec); err != nil {
			return n, io.ErrUnexpectedEOF
		}
		if err := visit(rec); err != nil {
			return n, err
		}
		pos = end
		n++
	}
	return n, nil
}
