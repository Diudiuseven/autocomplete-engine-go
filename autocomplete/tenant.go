package autocomplete

import "sort"

// tenantSnap is the immutable per-tenant view inside a Snapshot.
type tenantSnap struct {
	// entries maps normalized key -> immutable Entry.
	entries map[string]Entry
	// sortedKeys is entries' keys in lexicographic order. It makes bounded
	// scans deterministic: a ScanLimit always inspects the same prefix of keys
	// for a given committed version, regardless of map iteration order.
	sortedKeys []string
	// quota is the maximum len(entries); 0 means unlimited.
	quota int
	// stats holds tenant-local counters.
	stats TenantStats
}

// TenantStats are counters scoped to one tenant; they never aggregate across
// tenants.
type TenantStats struct {
	Candidates      int
	Updates         int64
	Deletes         int64
	Rejected        int64
	BatchesDone     int64
	Queries         int64
	QueriesTimedOut int64
}

func (ts *tenantSnap) rebuildKeys() {
	ts.sortedKeys = make([]string, 0, len(ts.entries))
	for k := range ts.entries {
		ts.sortedKeys = append(ts.sortedKeys, k)
	}
	sort.Strings(ts.sortedKeys)
}
