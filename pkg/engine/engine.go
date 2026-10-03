// Package engine implements the autocomplete service: deterministic ranking,
// prefix + substring matching, tenant isolation, quotas, and atomic
// batch imports on top of the MVCC store and WAL.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Diudiuseven/autocomplete-engine-go/pkg/clock"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/model"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/normalize"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/store"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/tenant"
	"github.com/Diudiuseven/autocomplete-engine-go/pkg/wal"
)

// Errors.
var (
	ErrEmptyTenant    = errors.New("tenant id must not be empty")
	ErrTenantExists   = errors.New("tenant already exists")
	ErrTenantNotFound = errors.New("tenant not found")
	ErrTermNotFound   = errors.New("term not found")
	ErrQuotaExceeded  = errors.New("tenant quota exceeded")
	ErrEmptyTerm      = errors.New("term must not be empty")
	ErrTermTooLong    = errors.New("term too long")
	ErrInvalidTerm    = errors.New("term contains invalid characters")
	ErrEmptyQuery     = errors.New("query must not be empty")
	ErrQueryTooLong   = errors.New("query too long")
	ErrInvalidQuery   = errors.New("query contains invalid characters")
	ErrQueryTimeout   = errors.New("query timeout")
	ErrScanLimit      = errors.New("scan limit exceeded")
	ErrNegativeDelta  = errors.New("heat and uses deltas must be non-negative")
	ErrBadConfig      = errors.New("bad config")
)

// Config holds all engine configuration.
type Config struct {
	// DataDir is the persistence directory (WAL + snapshot). Empty disables persistence.
	DataDir string
	// MaxResults caps the number of matches returned per query.
	MaxResults int
	// ScanLimit caps how many candidates a query may examine.
	ScanLimit int
	// QueryTimeout bounds query wall time.
	QueryTimeout time.Duration
	// MaxQueryLen rejects queries/terms longer than this many runes.
	MaxQueryLen int
	// HeatHalfLife is the heat decay half-life.
	HeatHalfLife time.Duration
	// RecencyHalfLife is the recency boost decay half-life.
	RecencyHalfLife time.Duration
	// RecencyWeight scales the recency boost added to the score.
	RecencyWeight float64
	// Clock is the decay/recency time source (nil => real clock).
	Clock clock.Clock
}

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{
		MaxResults:      20,
		ScanLimit:       10000,
		QueryTimeout:    200 * time.Millisecond,
		MaxQueryLen:     64,
		HeatHalfLife:    30 * 24 * time.Hour,
		RecencyHalfLife: 3 * 24 * time.Hour,
		RecencyWeight:   1.0,
	}
}

func (c *Config) fill() error {
	if c.MaxResults == 0 {
		c.MaxResults = 20
	}
	if c.ScanLimit == 0 {
		c.ScanLimit = 10000
	}
	if c.QueryTimeout == 0 {
		c.QueryTimeout = 200 * time.Millisecond
	}
	if c.MaxQueryLen == 0 {
		c.MaxQueryLen = 64
	}
	if c.HeatHalfLife == 0 {
		c.HeatHalfLife = 30 * 24 * time.Hour
	}
	if c.RecencyHalfLife == 0 {
		c.RecencyHalfLife = 3 * 24 * time.Hour
	}
	if c.RecencyWeight == 0 {
		c.RecencyWeight = 1.0
	}
	if c.MaxResults < 0 || c.ScanLimit < 0 || c.QueryTimeout < 0 || c.MaxQueryLen < 0 ||
		c.HeatHalfLife < 0 || c.RecencyHalfLife < 0 || c.RecencyWeight < 0 {
		return ErrBadConfig
	}
	if c.Clock == nil {
		c.Clock = clock.Real{}
	}
	return nil
}

// SearchOptions carries per-query overrides.
type SearchOptions struct {
	// Limit overrides MaxResults (<=0 means use config).
	Limit int
}

// SearchResult is a complete, deterministic query answer.
type SearchResult struct {
	Query   string        `json:"query"`
	Matches []model.Match `json:"matches"`
	Scanned int           `json:"scanned"`
	Version int64         `json:"version"`
}

// TenantStats is per-tenant aggregate state.
type TenantStats struct {
	Tenant     string `json:"tenant"`
	Candidates int64  `json:"candidates"`
	TotalUses  int64  `json:"total_uses"`
}

// Engine is the autocomplete service.
type Engine struct {
	cfg      Config
	store    *store.Store
	wal      *wal.WAL
	registry *tenant.Registry
	clock    clock.Clock
	nextTxID atomic.Int64
}

// New creates (or recovers) an engine.
func New(cfg Config) (*Engine, error) {
	if err := cfg.fill(); err != nil {
		return nil, err
	}
	e := &Engine{
		cfg:      cfg,
		store:    store.New(),
		registry: tenant.New(),
		clock:    cfg.Clock,
	}
	if cfg.DataDir != "" {
		w, err := wal.Open(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		e.wal = w
		if err := e.recover(); err != nil {
			w.Close()
			return nil, err
		}
	}
	return e, nil
}

// Close persists a checkpoint and closes the WAL.
func (e *Engine) Close() error {
	if e.wal == nil {
		return nil
	}
	if err := e.persist(); err != nil {
		return err
	}
	return e.wal.Close()
}

func (e *Engine) checkTenant(id string) error {
	if id == "" {
		return ErrEmptyTenant
	}
	if !e.registry.Exists(id) {
		return ErrTenantNotFound
	}
	return nil
}

func validateTerm(term string, maxLen int) error {
	if term == "" {
		return ErrEmptyTerm
	}
	if !normalize.Valid(term) {
		return ErrInvalidTerm
	}
	if normalize.RuneCount(term) > maxLen {
		return ErrTermTooLong
	}
	return nil
}

func validateQuery(q string, maxLen int) error {
	if q == "" {
		return ErrEmptyQuery
	}
	if !normalize.Valid(q) {
		return ErrInvalidQuery
	}
	if normalize.RuneCount(q) > maxLen {
		return ErrQueryTooLong
	}
	return nil
}

// CreateTenant registers a tenant with a quota.
func (e *Engine) CreateTenant(id string, q tenant.Quota) error {
	if id == "" {
		return ErrEmptyTenant
	}
	if !e.registry.Create(id, q) {
		return ErrTenantExists
	}
	return nil
}

// Lookup returns the stored candidate for a term (tests/diagnostics).
func (e *Engine) Lookup(tenantID, term string) (*model.Candidate, error) {
	if err := e.checkTenant(tenantID); err != nil {
		return nil, err
	}
	if err := validateTerm(term, e.cfg.MaxQueryLen); err != nil {
		return nil, err
	}
	view := e.store.Current().Tenant(tenantID)
	if view == nil {
		return nil, ErrTermNotFound
	}
	c := view.Candidates[normalize.Key(term)]
	if c == nil {
		return nil, ErrTermNotFound
	}
	return c, nil
}

// Upsert adds heat/uses to a term, creating it if absent. The WAL transaction
// is committed before the new snapshot is published; any failure leaves the
// committed version untouched.
func (e *Engine) Upsert(ctx context.Context, tenantID, term string, heat float64, uses int64, when time.Time) error {
	if err := e.checkTenant(tenantID); err != nil {
		return err
	}
	if err := validateTerm(term, e.cfg.MaxQueryLen); err != nil {
		return err
	}
	if heat < 0 || uses < 0 {
		return ErrNegativeDelta
	}
	if when.IsZero() {
		when = e.clock.Now()
	}
	norm := normalize.Key(term)
	txID := e.nextTxID.Add(1)

	return e.store.Update(func(cur *store.Snapshot) (*store.Snapshot, error) {
		view := cur.Tenant(tenantID)
		if view == nil {
			view = &store.TenantView{}
		}
		cands := cloneCandidates(view)
		if _, exists := cands[norm]; !exists {
			if q, ok := e.registry.QuotaOf(tenantID); ok && q.MaxCandidates > 0 && int64(len(cands)) >= q.MaxCandidates {
				return nil, ErrQuotaExceeded
			}
		}
		c := cands[norm]
		if c == nil {
			c = &model.Candidate{Term: term, Norm: norm, Heat: heat, Uses: uses, LastUsed: when, UpdatedAt: when}
		} else {
			nc := *c
			nc.Heat += heat
			nc.Uses += uses
			if when.After(nc.LastUsed) {
				nc.LastUsed = when
			}
			nc.UpdatedAt = when
			c = &nc
		}
		cands[norm] = c

		if err := e.commitTxn(txID, []wal.Record{{
			TxID: txID, Op: wal.OpPut, Tenant: tenantID, Term: term, Heat: heat, Uses: uses, When: when.UnixNano(),
		}}); err != nil {
			return nil, err
		}

		tenants := cloneTenants(cur)
		tenants[tenantID] = buildView(cands)
		return store.NewSnapshot(cur.Version+1, txID, e.clock.Now(), tenants), nil
	})
}

// Delete removes a term. Missing terms are rejected; the committed version is
// untouched on failure.
func (e *Engine) Delete(ctx context.Context, tenantID, term string) error {
	if err := e.checkTenant(tenantID); err != nil {
		return err
	}
	if err := validateTerm(term, e.cfg.MaxQueryLen); err != nil {
		return err
	}
	norm := normalize.Key(term)
	txID := e.nextTxID.Add(1)

	return e.store.Update(func(cur *store.Snapshot) (*store.Snapshot, error) {
		view := cur.Tenant(tenantID)
		if view == nil || view.Candidates[norm] == nil {
			return nil, ErrTermNotFound
		}
		cands := cloneCandidates(view)
		delete(cands, norm)

		if err := e.commitTxn(txID, []wal.Record{{
			TxID: txID, Op: wal.OpDelete, Tenant: tenantID, Term: term, When: e.clock.Now().UnixNano(),
		}}); err != nil {
			return nil, err
		}

		tenants := cloneTenants(cur)
		tenants[tenantID] = buildView(cands)
		return store.NewSnapshot(cur.Version+1, txID, e.clock.Now(), tenants), nil
	})
}

// commitTxn writes begin/records/commit. With no WAL configured it is a no-op.
func (e *Engine) commitTxn(txID int64, recs []wal.Record) error {
	if e.wal == nil {
		return nil
	}
	if err := e.wal.Begin(txID); err != nil {
		return err
	}
	for _, r := range recs {
		if r.Op == wal.OpDelete {
			if err := e.wal.Delete(r); err != nil {
				return err
			}
		} else {
			if err := e.wal.Put(r); err != nil {
				return err
			}
		}
	}
	return e.wal.Commit(txID)
}

// score computes the deterministic ranking score:
//
//	score = Heat * 2^(-age/HeatHalfLife) + RecencyWeight * 2^(-age/RecencyHalfLife)
//
// where age = now - LastUsed. Heat decays with the configured half-life; the
// recency boost decays faster. The time source is the injected Clock.
func (e *Engine) score(c *model.Candidate, now time.Time) float64 {
	age := now.Sub(c.LastUsed)
	if age < 0 {
		age = 0
	}
	heatDecay := math.Pow(0.5, float64(age)/float64(e.cfg.HeatHalfLife))
	recDecay := math.Pow(0.5, float64(age)/float64(e.cfg.RecencyHalfLife))
	return c.Heat*heatDecay + e.cfg.RecencyWeight*recDecay
}

// Search runs a prefix + substring query within the configured scan budget
// and timeout. On budget exhaustion it returns a distinguishable error rather
// than partial results.
func (e *Engine) Search(ctx context.Context, tenantID, query string, opts SearchOptions) (*SearchResult, error) {
	if err := e.checkTenant(tenantID); err != nil {
		return nil, err
	}
	if err := validateQuery(query, e.cfg.MaxQueryLen); err != nil {
		return nil, err
	}
	limit := e.cfg.MaxResults
	if opts.Limit > 0 {
		limit = opts.Limit
	}

	ctx, cancel := context.WithTimeout(ctx, e.cfg.QueryTimeout)
	defer cancel()

	snap := e.store.Current()
	view := snap.Tenant(tenantID)
	if view == nil {
		view = &store.TenantView{}
	}
	q := normalize.Key(query)
	now := e.clock.Now()

	matches := make([]model.Match, 0, min(len(view.Candidates), limit))
	scanned := 0
	for _, key := range view.Keys {
		if scanned >= e.cfg.ScanLimit {
			return nil, ErrScanLimit
		}
		if ctx.Err() != nil {
			return nil, ErrQueryTimeout
		}
		c := view.Candidates[key]
		if contains(key, q) {
			matches = append(matches, model.Match{
				Term:  c.Term,
				Score: e.score(c, now),
				Heat:  c.Heat,
				Uses:  c.Uses,
			})
		}
		scanned++
	}
	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].Score != matches[j].Score {
			return matches[i].Score > matches[j].Score
		}
		return matches[i].Term < matches[j].Term
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	return &SearchResult{Query: query, Matches: matches, Scanned: scanned, Version: snap.Version}, nil
}

// contains reports whether sub is a substring of s (prefix match is the
// substring at position 0, so one check covers both).
func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Stats returns per-tenant counters.
func (e *Engine) Stats(tenantID string) (TenantStats, error) {
	if err := e.checkTenant(tenantID); err != nil {
		return TenantStats{}, err
	}
	snap := e.store.Current()
	view := snap.Tenant(tenantID)
	var totalUses int64
	var n int64
	if view != nil {
		n = int64(len(view.Candidates))
		for _, c := range view.Candidates {
			totalUses += c.Uses
		}
	}
	return TenantStats{Tenant: tenantID, Candidates: n, TotalUses: totalUses}, nil
}

// ---------- snapshot / recovery ----------

type persistedState struct {
	Version int64                      `json:"version"`
	TxID    int64                      `json:"txid"`
	Tenants map[string]persistedTenant `json:"tenants"`
}

type persistedTenant struct {
	Quota      tenant.Quota       `json:"quota"`
	Candidates []*model.Candidate `json:"candidates"`
}

func (e *Engine) persist() error {
	snap := e.store.Current()
	state := persistedState{
		Version: snap.Version,
		TxID:    snap.TxID,
		Tenants: map[string]persistedTenant{},
	}
	for id, view := range snap.Tenants() {
		pt := persistedTenant{Quota: tenant.Quota{}}
		if q, ok := e.registry.QuotaOf(id); ok {
			pt.Quota = q
		}
		for _, c := range view.Candidates {
			pt.Candidates = append(pt.Candidates, c)
		}
		state.Tenants[id] = pt
	}
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return e.wal.Checkpoint(snap.TxID, b)
}

// recover loads the last checkpoint and replays committed WAL transactions.
func (e *Engine) recover() error {
	baseTxID, err := e.wal.CheckpointTxID()
	if err != nil {
		return err
	}
	tenants := map[string]*store.TenantView{}
	version := int64(0)

	if baseTxID > 0 {
		b, err := e.wal.LoadSnapshot()
		if err != nil {
			return err
		}
		if b == nil {
			return errors.New("checkpoint marker without snapshot file")
		}
		var state persistedState
		if err := json.Unmarshal(b, &state); err != nil {
			return err
		}
		version = state.Version
		for id, pt := range state.Tenants {
			cands := map[string]*model.Candidate{}
			for _, c := range pt.Candidates {
				cands[c.Norm] = c
			}
			tenants[id] = buildView(cands)
			e.registry.Create(id, pt.Quota)
		}
	}

	lastTxID := int64(-1)
	committed, maxSeen, err := e.wal.Replay(baseTxID, func(r wal.Record) error {
		if r.TxID != lastTxID {
			version++
			lastTxID = r.TxID
		}
		view := tenants[r.Tenant]
		if view == nil {
			view = &store.TenantView{}
		}
		cands := cloneCandidates(view)
		switch r.Op {
		case wal.OpPut:
			when := time.Unix(0, r.When)
			norm := normalize.Key(r.Term)
			c := cands[norm]
			if c == nil {
				c = &model.Candidate{Term: r.Term, Norm: norm, Heat: r.Heat, Uses: r.Uses, LastUsed: when, UpdatedAt: when}
			} else {
				nc := *c
				nc.Heat += r.Heat
				nc.Uses += r.Uses
				if when.After(nc.LastUsed) {
					nc.LastUsed = when
				}
				nc.UpdatedAt = when
				c = &nc
			}
			cands[norm] = c
		case wal.OpDelete:
			delete(cands, normalize.Key(r.Term))
		}
		tenants[r.Tenant] = buildView(cands)
		return nil
	})
	if err != nil {
		return err
	}
	_ = committed

	for id := range tenants {
		e.registry.Create(id, tenant.Quota{})
	}
	if err := e.store.Update(func(cur *store.Snapshot) (*store.Snapshot, error) {
		return store.NewSnapshot(version, committed, e.clock.Now(), tenants), nil
	}); err != nil {
		return err
	}
	e.nextTxID.Store(max(baseTxID, maxSeen) + 1)
	return nil
}

// ---------- helpers ----------

func buildView(cands map[string]*model.Candidate) *store.TenantView {
	keys := make([]string, 0, len(cands))
	for k := range cands {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return &store.TenantView{Candidates: cands, Keys: keys}
}

func cloneCandidates(view *store.TenantView) map[string]*model.Candidate {
	out := make(map[string]*model.Candidate, len(view.Candidates))
	for k, v := range view.Candidates {
		out[k] = v
	}
	return out
}

func cloneTenants(cur *store.Snapshot) map[string]*store.TenantView {
	out := make(map[string]*store.TenantView, len(cur.Tenants()))
	for k, v := range cur.Tenants() {
		out[k] = v
	}
	return out
}
