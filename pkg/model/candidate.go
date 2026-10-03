// Package model defines the core data types.
package model

import "time"

// Candidate is a single indexed term. It is treated as immutable once stored;
// updates produce a new Candidate rather than mutating an existing one.
type Candidate struct {
	// Term is the original display form (never modified).
	Term string `json:"term"`
	// Norm is the normalized matching key (see normalize.Key).
	Norm string `json:"norm"`
	// Heat is the accumulated historical popularity.
	Heat float64 `json:"heat"`
	// Uses is the accumulated usage count.
	Uses int64 `json:"uses"`
	// LastUsed is the most recent usage time; the decay baseline is now - LastUsed.
	LastUsed time.Time `json:"last_used"`
	// UpdatedAt is the last write time (for diagnostics).
	UpdatedAt time.Time `json:"updated_at"`
}

// Entry is one item in a batch import.
type Entry struct {
	Term string
	Heat float64
	Uses int64
	When time.Time // zero means "use engine clock"
}

// Match is one search hit.
type Match struct {
	Term  string  `json:"term"`
	Score float64 `json:"score"`
	Heat  float64 `json:"heat"`
	Uses  int64   `json:"uses"`
}
