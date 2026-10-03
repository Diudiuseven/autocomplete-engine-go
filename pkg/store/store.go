// Package store implements MVCC-style immutable snapshots.
//
// A published Snapshot is never mutated: writes copy the affected tenant's
// view (copy-on-write), apply the change to the copy, and publish the new
// snapshot via an atomic pointer swap. Queries load the pointer without
// taking the write lock, so they always observe one complete committed
// version and can never see a half-written candidate.
package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Diudiuseven/autocomplete-engine-go/pkg/model"
)

// TenantView is the immutable per-tenant index inside a Snapshot.
type TenantView struct {
	// Candidates maps normalized key -> candidate. The map is never mutated
	// after publication; writes replace it with a copy.
	Candidates map[string]*model.Candidate
	// Keys is the sorted list of normalized keys for deterministic scans.
	Keys []string
}

// Snapshot is one complete committed version of the whole index.
type Snapshot struct {
	Version int64
	TxID    int64
	TakenAt time.Time
	tenants map[string]*TenantView
}

// NewSnapshot builds a snapshot. The caller must not mutate tenants or the
// candidate maps afterwards (they become shared immutable state).
func NewSnapshot(version, txID int64, takenAt time.Time, tenants map[string]*TenantView) *Snapshot {
	return &Snapshot{Version: version, TxID: txID, TakenAt: takenAt, tenants: tenants}
}

// Tenant returns the immutable view for a tenant (nil if absent).
func (s *Snapshot) Tenant(id string) *TenantView { return s.tenants[id] }

// Tenants returns the immutable tenant map (never nil).
func (s *Snapshot) Tenants() map[string]*TenantView { return s.tenants }

// Store holds the current snapshot pointer and serializes writers.
type Store struct {
	mu  sync.Mutex
	cur atomic.Pointer[Snapshot]
}

// New returns an empty store.
func New() *Store {
	s := &Store{}
	s.cur.Store(NewSnapshot(0, 0, time.Time{}, map[string]*TenantView{}))
	return s
}

// Current returns the latest committed snapshot. Never returns nil.
func (s *Store) Current() *Snapshot { return s.cur.Load() }

// Update runs fn under the write lock. fn receives the current snapshot and
// returns the snapshot to publish; returning an error aborts without
// publishing. Callers must not mutate the received snapshot.
func (s *Store) Update(fn func(cur *Snapshot) (*Snapshot, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next, err := fn(s.cur.Load())
	if err != nil {
		return err
	}
	s.cur.Store(next)
	return nil
}
