package autocomplete

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BatchItem is one candidate offered by an import source or a direct update.
type BatchItem struct {
	// Display is the original surface form. The normalized form derived from
	// it is the identity key.
	Display string
	// Heat is the popularity mass contributed by this item. Negative values
	// are rejected by the Engine.
	Heat float64
	// UseCount is the number of uses contributed by this item.
	UseCount int64
	// LastUsedAt optionally pins the recency timestamp; zero means "now" at
	// commit time.
	LastUsedAt time.Time
}

// batchState tracks lifecycle so double commit/use-after-abort are rejected.
type batchState uint8

const (
	batchActive batchState = iota
	batchCommitting
	batchCommitted
	batchAborted
)

// Batch is an in-progress import. Repeated keys inside one batch accumulate;
// nothing is visible to queries until Commit publishes the whole batch in one
// atomic mutation. AddToBatch/CommitBatch/AbortBatch are safe to invoke
// concurrently; they synchronize on mu (the typical pattern remains one
// draining goroutine, as in Import).
type Batch struct {
	mu     sync.Mutex
	id     string
	tenant string
	items  []BatchItem
	state  batchState
}

// ID returns the batch's unique identifier (also used in the WAL).
func (b *Batch) ID() string { return b.id }

// Len returns how many items have been added so far.
func (b *Batch) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

// ImportSource yields import data; local files and in-memory channels both
// implement it. Implementations must be safe for single-goroutine use.
type ImportSource interface {
	Next() (BatchItem, error) // io.EOF at end
	Close() error
}

// MemorySource serves items already held in memory.
type MemorySource struct {
	items []BatchItem
	pos   int
}

// NewMemorySource returns an ImportSource over a slice copy.
func NewMemorySource(items []BatchItem) *MemorySource {
	cp := make([]BatchItem, len(items))
	copy(cp, items)
	return &MemorySource{items: cp}
}

func (m *MemorySource) Next() (BatchItem, error) {
	if m.pos >= len(m.items) {
		return BatchItem{}, io.EOF
	}
	it := m.items[m.pos]
	m.pos++
	return it, nil
}
func (m *MemorySource) Close() error { return nil }

// ChannelSource adapts an in-memory pipe (chan) to ImportSource.
type ChannelSource struct {
	ch  <-chan BatchItem
	err error
}

// NewChannelSource returns an ImportSource reading ch until it closes.
func NewChannelSource(ch <-chan BatchItem) *ChannelSource { return &ChannelSource{ch: ch} }

func (c *ChannelSource) Next() (BatchItem, error) {
	if it, ok := <-c.ch; ok {
		return it, nil
	}
	if c.err != nil {
		return BatchItem{}, c.err
	}
	return BatchItem{}, io.EOF
}
func (c *ChannelSource) Close() error { return nil }

// FileSource reads a local UTF-8 text file. One candidate per line, fields
// separated by tabs:
//
//	display
//	display\theat
//	display\theat\tuseCount
//	display\theat\tuseCount\tlastUsedRFC3339
//
// Blank lines and lines starting with '#' are ignored. A malformed line
// yields an error wrapping ErrInvalidInput from Next (it does not silently
// corrupt the import).
type FileSource struct {
	f  *os.File
	sc *bufio.Scanner
	ln int
}

// NewFileSource opens path for import.
func NewFileSource(path string) (*FileSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("autocomplete: import file: %w", err)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	return &FileSource{f: f, sc: sc}, nil
}

func (fs *FileSource) Next() (BatchItem, error) {
	for fs.sc.Scan() {
		fs.ln++
		line := fs.sc.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		it := BatchItem{Display: fields[0], Heat: 1, UseCount: 1}
		if len(fields) > 1 && fields[1] != "" {
			v, err := strconv.ParseFloat(fields[1], 64)
			if err != nil || v < 0 {
				return BatchItem{}, fmt.Errorf("%w: line %d bad heat %q", ErrInvalidInput, fs.ln, fields[1])
			}
			it.Heat = v
		}
		if len(fields) > 2 && fields[2] != "" {
			v, err := strconv.ParseInt(fields[2], 10, 64)
			if err != nil || v < 0 {
				return BatchItem{}, fmt.Errorf("%w: line %d bad use count %q", ErrInvalidInput, fs.ln, fields[2])
			}
			it.UseCount = v
		}
		if len(fields) > 3 && fields[3] != "" {
			t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(fields[3]))
			if err != nil {
				return BatchItem{}, fmt.Errorf("%w: line %d bad timestamp %q", ErrInvalidInput, fs.ln, fields[3])
			}
			it.LastUsedAt = t.UTC()
		}
		return it, nil
	}
	if err := fs.sc.Err(); err != nil {
		return BatchItem{}, fmt.Errorf("autocomplete: read import file: %w", err)
	}
	return BatchItem{}, io.EOF
}

func (fs *FileSource) Close() error {
	if fs.f == nil {
		return nil
	}
	err := fs.f.Close()
	fs.f = nil
	return err
}
