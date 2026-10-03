package autocomplete

import "time"

// Entry is one candidate term belonging to one tenant. It is immutable once
// published inside a Snapshot; updates create replacement copies.
type Entry struct {
	// Tenant scopes visibility. Entries of different tenants never share an
	// identity and are never visible across tenants.
	Tenant string
	// Key is the normalized identity of the term (see Normalize).
	Key string
	// Display preserves the original surface form supplied on the most recent
	// accepted update. Matching never uses it.
	Display string
	// Heat is the accumulated, undecayed popularity mass. Decay is applied at
	// query time only; the stored value itself never changes with wall clock.
	Heat float64
	// UseCount is the accumulated number of uses.
	UseCount int64
	// CreatedAt is the first-seen wall-clock time.
	CreatedAt time.Time
	// LastUsedAt is the time of the last accumulated use; it drives both heat
	// decay reference and the recency component.
	LastUsedAt time.Time
}
