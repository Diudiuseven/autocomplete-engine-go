// Package tenant manages tenant registration and quotas.
// Tenant data is physically namespaced; every engine operation is scoped to
// one tenant and cross-tenant access has no API path.
package tenant

import "sync"

// Quota limits per-tenant resources. Zero values mean unlimited.
type Quota struct {
	MaxCandidates int64
}

// Registry is the tenant directory.
type Registry struct {
	mu      sync.RWMutex
	tenants map[string]Quota
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{tenants: map[string]Quota{}}
}

// Create registers a tenant. Returns false if it already exists.
func (r *Registry) Create(id string, q Quota) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tenants[id]; ok {
		return false
	}
	r.tenants[id] = q
	return true
}

// Exists reports whether id is registered.
func (r *Registry) Exists(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.tenants[id]
	return ok
}

// QuotaOf returns the tenant's quota.
func (r *Registry) QuotaOf(id string) (Quota, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	q, ok := r.tenants[id]
	return q, ok
}

// All returns a snapshot copy of all tenant quotas.
func (r *Registry) All() map[string]Quota {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]Quota, len(r.tenants))
	for k, v := range r.tenants {
		out[k] = v
	}
	return out
}

// Replace swaps the whole registry (used during recovery).
func (r *Registry) Replace(m map[string]Quota) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenants = make(map[string]Quota, len(m))
	for k, v := range m {
		r.tenants[k] = v
	}
}
