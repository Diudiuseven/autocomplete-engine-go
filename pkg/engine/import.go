package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/Diudiuseven/autocomplete-engine-go/pkg/model"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/normalize"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/store"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/wal"
)

// ImportSource streams batch entries.
type ImportSource interface {
	// Next returns the next entry. io.EOF marks clean end.
	Next() (model.Entry, error)
}

// ImportResult summarizes a committed batch.
type ImportResult struct {
	Entries int64 `json:"entries"` // lines read
	Upserts int64 `json:"upserts"` // distinct terms written
}

// agg is an in-batch accumulator keyed by normalized term.
type agg struct {
	term string
	heat float64
	uses int64
	when time.Time
}

// Import runs a batch import as a single committed transaction: duplicate
// terms within the batch accumulate heat/uses (they are never overwritten);
// on any failure nothing is committed.
func (e *Engine) Import(ctx context.Context, tenantID string, src ImportSource) (ImportResult, error) {
	if err := e.checkTenant(tenantID); err != nil {
		return ImportResult{}, err
	}

	acc := map[string]*agg{}
	var entries int64
	for {
		if err := ctx.Err(); err != nil {
			return ImportResult{}, err
		}
		ent, err := src.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return ImportResult{}, err
		}
		if err := validateTerm(ent.Term, e.cfg.MaxQueryLen); err != nil {
			return ImportResult{}, fmt.Errorf("entry %d: %w", entries, err)
		}
		if ent.Heat < 0 || ent.Uses < 0 {
			return ImportResult{}, fmt.Errorf("entry %d: %w", entries, ErrNegativeDelta)
		}
		if ent.When.IsZero() {
			ent.When = e.clock.Now()
		}
		entries++
		norm := normalize.Key(ent.Term)
		if a, ok := acc[norm]; ok {
			a.heat += ent.Heat
			a.uses += ent.Uses
			if ent.When.After(a.when) {
				a.when = ent.When
			}
		} else {
			acc[norm] = &agg{term: ent.Term, heat: ent.Heat, uses: ent.Uses, when: ent.When}
		}
	}

	txID := e.nextTxID.Add(1)
	recs := make([]wal.Record, 0, len(acc))
	for _, a := range acc {
		recs = append(recs, wal.Record{
			TxID: txID, Op: wal.OpPut, Tenant: tenantID, Term: a.term, Heat: a.heat, Uses: a.uses, When: a.when.UnixNano(),
		})
	}

	err := e.store.Update(func(cur *store.Snapshot) (*store.Snapshot, error) {
		view := cur.Tenant(tenantID)
		if view == nil {
			view = &store.TenantView{}
		}
		cands := cloneCandidates(view)
		now := e.clock.Now()
		for _, a := range acc {
			norm := normalize.Key(a.term)
			c := cands[norm]
			if c == nil {
				c = &model.Candidate{Term: a.term, Norm: norm, Heat: a.heat, Uses: a.uses, LastUsed: a.when, UpdatedAt: now}
			} else {
				nc := *c
				nc.Heat += a.heat
				nc.Uses += a.uses
				if a.when.After(nc.LastUsed) {
					nc.LastUsed = a.when
				}
				nc.UpdatedAt = now
				c = &nc
			}
			cands[norm] = c
		}
		if q, ok := e.registry.QuotaOf(tenantID); ok && q.MaxCandidates > 0 && int64(len(cands)) > q.MaxCandidates {
			return nil, ErrQuotaExceeded
		}

		if err := e.commitTxn(txID, recs); err != nil {
			return nil, err
		}

		tenants := cloneTenants(cur)
		tenants[tenantID] = buildView(cands)
		return store.NewSnapshot(cur.Version+1, txID, now, tenants), nil
	})
	if err != nil {
		return ImportResult{}, err
	}
	return ImportResult{Entries: entries, Upserts: int64(len(acc))}, nil
}

// FileSource reads tab- or comma-separated lines: term,heat,uses.
// Blank lines and lines starting with '#' are ignored.
type FileSource struct {
	r *bufio.Reader
}

// NewFileSource wraps a reader.
func NewFileSource(r io.Reader) *FileSource {
	return &FileSource{r: bufio.NewReader(r)}
}

// Next implements ImportSource.
func (s *FileSource) Next() (model.Entry, error) {
	for {
		line, err := s.r.ReadString('\n')
		if err != nil && line == "" {
			return model.Entry{}, err
		}
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			parts := strings.Split(line, "\t")
			if len(parts) == 1 {
				parts = strings.Split(line, ",")
			}
			if len(parts) > 3 {
				return model.Entry{}, fmt.Errorf("bad line: %q", line)
			}
			ent := model.Entry{Term: strings.TrimSpace(parts[0])}
			if len(parts) >= 2 && strings.TrimSpace(parts[1]) != "" {
				v, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
				if err != nil {
					return model.Entry{}, fmt.Errorf("bad heat %q: %w", parts[1], err)
				}
				ent.Heat = v
			}
			if len(parts) == 3 && strings.TrimSpace(parts[2]) != "" {
				v, err := strconv.ParseInt(strings.TrimSpace(parts[2]), 10, 64)
				if err != nil {
					return model.Entry{}, fmt.Errorf("bad uses %q: %w", parts[2], err)
				}
				ent.Uses = v
			}
			return ent, nil
		}
		if err != nil {
			return model.Entry{}, err
		}
	}
}

// PipeSource is an in-memory channel source for tests/streaming.
type PipeSource struct {
	ch <-chan model.Entry
}

// NewPipeSource returns a pipe and its feeder channel.
func NewPipeSource(buf int) (*PipeSource, chan<- model.Entry) {
	ch := make(chan model.Entry, buf)
	return &PipeSource{ch: ch}, ch
}

// Next implements ImportSource.
func (s *PipeSource) Next() (model.Entry, error) {
	e, ok := <-s.ch
	if !ok {
		return model.Entry{}, io.EOF
	}
	return e, nil
}
