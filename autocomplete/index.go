package autocomplete

import (
	"sort"
	"strings"
	"time"
)

// Clock is the single, explicit source of "now". Heat-decay references and
// all logical timestamps come from here; production uses SystemClock and tests
// use a controllable fake. Wall-clock query deadlines always use the real
// clock, because timeout protects real latency.
type Clock interface {
	Now() time.Time
}

// SystemClock returns the real wall-clock time in UTC.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// Candidate is one query result with scoring metadata.
type Candidate struct {
	Entry Entry
	Score float64
	Match MatchKind
}

// MatchKind distinguishes where the query matched inside the term.
type MatchKind uint8

const (
	MatchPrefix MatchKind = iota + 1
	MatchInfix
)

// QueryOptions carries per-query overrides; zero values inherit engine config.
type QueryOptions struct {
	MaxCandidates int
	ScanLimit     int
	Timeout       time.Duration
}

// QueryStats reports how much work one query did.
type QueryStats struct {
	Scanned  int
	Matched  int
	Version  uint64
	TimedOut bool
}

// mutation is one atomic change set applied to a cloned Snapshot.
type mutation struct {
	tenant  string
	upserts []Entry
	deletes []string
	// updatesStats counts accepted upserts; deletedStats counts deletes that
	// removed an existing key.
}

// Index owns the published Snapshot and applies mutations. The Engine
// serializes publish calls; Query calls are lock-free.
type Index struct {
	cfg    Config
	scorer *Scorer
	clock  Clock
	ref    snapshotRef
}

func newIndex(cfg Config, scorer *Scorer, clock Clock, initial *Snapshot) *Index {
	idx := &Index{cfg: cfg, scorer: scorer, clock: clock}
	if initial == nil {
		initial = newSnapshot()
	}
	idx.ref.store(initial)
	return idx
}

func (idx *Index) current() *Snapshot { return idx.ref.load() }

// publish applies m to a private clone and atomically swaps it in. On error
// (unknown tenant, quota) the published Snapshot is untouched; a failed
// update can never pollute the committed version.
func (idx *Index) publish(m mutation) (*Snapshot, error) {
	cur := idx.ref.load()
	ts := cur.tenant(m.tenant)
	if ts == nil {
		return nil, ErrUnknownTenant
	}

	next := cur.clone()
	nts := next.tenants[m.tenant]

	upsertCount, deleteCount := 0, 0
	for _, ne := range m.upserts {
		if old, ok := nts.entries[ne.Key]; ok {
			ne = accumulate(old, ne)
		}
		nts.entries[ne.Key] = ne
		upsertCount++
	}
	for _, k := range m.deletes {
		if _, ok := nts.entries[k]; ok {
			delete(nts.entries, k)
			deleteCount++
		}
	}

	if nts.quota > 0 && len(nts.entries) > nts.quota {
		// The clone is discarded; the published snapshot is unchanged.
		return nil, ErrTenantQuotaExceeded
	}

	nts.stats.Updates += int64(upsertCount)
	nts.stats.Deletes += int64(deleteCount)
	nts.stats.Candidates = len(nts.entries)

	// Deterministic key order for bounded scans (see Query).
	nts.sortedKeys = sortedKeysOf(nts.entries)

	next.committedAt = idx.clock.Now()
	idx.ref.store(next)
	return next, nil
}

// registerTenant adds a new tenant. It returns ErrBatchConflict if the tenant
// already exists; a re-register with a changed quota is ignored at this layer
// (the Engine owns that policy).
func (idx *Index) registerTenant(id string, quota int) (*Snapshot, error) {
	cur := idx.ref.load()
	if _, ok := cur.tenants[id]; ok {
		return nil, ErrBatchConflict
	}
	next := cur.clone()
	next.tenants[id] = &tenantSnap{
		entries:    map[string]Entry{},
		sortedKeys: []string{},
		quota:      quota,
	}
	next.committedAt = idx.clock.Now()
	idx.ref.store(next)
	return next, nil
}

// ensureTenant loads or lazily creates a tenant snapshot during recovery.
func ensureTenant(s *Snapshot, id string, quota int) *tenantSnap {
	if ts, ok := s.tenants[id]; ok {
		return ts
	}
	ts := &tenantSnap{entries: map[string]Entry{}, sortedKeys: []string{}, quota: quota}
	s.tenants[id] = ts
	return ts
}

// accumulate merges an incoming update into the existing entry. Heat and
// use count always ADD (a duplicate import never overwrites); the display
// form follows the most recently used update; identity timestamps follow the
// latest usage.
func accumulate(old, ne Entry) Entry {
	old.Heat += ne.Heat
	old.UseCount += ne.UseCount
	old.Display = ne.Display
	if ne.LastUsedAt.After(old.LastUsedAt) {
		old.LastUsedAt = ne.LastUsedAt
	}
	return old
}

func sortedKeysOf(m map[string]Entry) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// deadlineCheckInterval bounds how much work a query does between timeout
// checks.
const deadlineCheckInterval = 64

// Query scans only the named tenant's committed snapshot. It takes one
// logical timestamp for all scoring, traverses keys in sorted order so that
// ScanLimit selects a deterministic subset, and returns ErrQueryTimeout
// without partial data when the wall-clock deadline is exceeded.
func (idx *Index) Query(tenant, normQuery string, opts QueryOptions) ([]Candidate, QueryStats, error) {
	max := opts.MaxCandidates
	if max <= 0 {
		max = idx.cfg.MaxCandidates
	}
	limit := opts.ScanLimit
	if limit <= 0 {
		limit = idx.cfg.ScanLimit
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = idx.cfg.QueryTimeout
	}
	deadline := time.Now().Add(timeout)
	now := idx.clock.Now()

	snap := idx.ref.load()
	ts := snap.tenant(tenant)
	if ts == nil {
		return nil, QueryStats{Version: snap.version}, ErrUnknownTenant
	}

	// Bounded buffer: at most one Candidate per scanned entry.
	matches := make([]Candidate, 0, min(limit, len(ts.sortedKeys)))
	scanned := 0
	timedOut := false
	for _, k := range ts.sortedKeys {
		if scanned >= limit {
			break
		}
		if scanned&(deadlineCheckInterval-1) == 0 && time.Now().After(deadline) {
			timedOut = true
			break
		}
		scanned++
		kind := matchKind(k, normQuery)
		if kind == 0 {
			continue
		}
		e := ts.entries[k]
		matches = append(matches, Candidate{Entry: e, Score: idx.scorer.Score(e, now), Match: kind})
	}
	if !timedOut && time.Now().After(deadline) {
		timedOut = true
	}
	if timedOut {
		return nil, QueryStats{Scanned: scanned, Matched: len(matches), Version: snap.version, TimedOut: true}, ErrQueryTimeout
	}

	sort.SliceStable(matches, func(i, j int) bool {
		return idx.scorer.Less(matches[i], matches[j])
	})
	if len(matches) > max {
		matches = matches[:max]
	}
	return matches, QueryStats{Scanned: scanned, Matched: len(matches), Version: snap.version}, nil
}

// matchKind returns MatchPrefix when q is a prefix of key, MatchInfix when it
// occurs elsewhere, or 0 when absent. Empty q is rejected by the Engine.
func matchKind(key, q string) MatchKind {
	if strings.HasPrefix(key, q) {
		return MatchPrefix
	}
	if strings.Contains(key, q) {
		return MatchInfix
	}
	return 0
}
