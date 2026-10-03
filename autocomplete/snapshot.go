package autocomplete

import (
	"sync/atomic"
	"time"
)

// Snapshot is one immutable, committed version of the whole index. A query
// only ever reads a complete Snapshot; in-flight updates first clone the
// published Snapshot, mutate the private clone, then publish it with a single
// atomic pointer swap. Readers consequently never observe a half-written map
// and can never see field-level mixtures of an entry.
type Snapshot struct {
	// tenants maps tenant id -> immutable tenant snapshot.
	tenants map[string]*tenantSnap
	// version is the monotonically increasing commit version.
	version uint64
	// committedAt is when this snapshot was published.
	committedAt time.Time
}

func newSnapshot() *Snapshot {
	return &Snapshot{tenants: map[string]*tenantSnap{}}
}

func (s *Snapshot) Version() uint64              { return s.version }
func (s *Snapshot) CommittedAt() time.Time       { return s.committedAt }
func (s *Snapshot) tenant(id string) *tenantSnap { return s.tenants[id] }

// clone returns a private, deeply-copied snapshot suitable for mutation. Map
// contents (Entry values) are copied as well, so sharing never leaks between
// versions.
func (s *Snapshot) clone() *Snapshot {
	out := &Snapshot{
		version:     s.version + 1,
		committedAt: s.committedAt,
		tenants:     make(map[string]*tenantSnap, len(s.tenants)),
	}
	for id, ts := range s.tenants {
		entries := make(map[string]Entry, len(ts.entries))
		for k, e := range ts.entries {
			entries[k] = e
		}
		keys := append([]string(nil), ts.sortedKeys...)
		out.tenants[id] = &tenantSnap{
			entries:    entries,
			sortedKeys: keys,
			quota:      ts.quota,
			stats:      ts.stats,
		}
	}
	return out
}

// snapshotRef is an atomic holder for the currently published Snapshot.
type snapshotRef struct {
	p atomic.Pointer[Snapshot]
}

func (r *snapshotRef) load() *Snapshot   { return r.p.Load() }
func (r *snapshotRef) store(s *Snapshot) { r.p.Store(s) }
