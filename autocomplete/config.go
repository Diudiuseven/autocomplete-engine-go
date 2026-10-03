package autocomplete

import (
	"errors"
	"time"
)

// Config holds every tunable of the autocomplete service. The zero value is
// not usable; obtain one from DefaultConfig and override fields as needed.
//
// All duration-based semantics are anchored to the engine Clock
// (SystemClock in production): heat decay uses the entry's LastUsedAt as its
// fixed reference and the query time from the same clock as observation point.
type Config struct {
	// MaxCandidates caps the number of candidates returned by one query.
	MaxCandidates int
	// ScanLimit bounds how many candidate entries one query may inspect. The
	// bound is per query and protects latency/memory; it does not block writes.
	ScanLimit int
	// QueryTimeout bounds the wall-clock duration of one query. On timeout the
	// query returns ErrQueryTimeout (never a partial/half-sorted list).
	QueryTimeout time.Duration
	// MaxInputRunes rejects query/term inputs whose normalized rune count
	// exceeds it, with ErrTooLong.
	MaxInputRunes int

	// HalfLife controls exponential heat decay. At observation time t the
	// effective heat is Heat * 0.5^((t-LastUsedAt)/HalfLife). Decay is a
	// read-time projection; stored Heat is the accumulated undecayed mass.
	HalfLife time.Duration
	// HeatWeight and FreshnessWeight blend the two ranking components. Both
	// must be >= 0 and at least one must be > 0.
	//
	//	score = HeatWeight*decayedHeat
	//	      + FreshnessWeight*max(0, 1 - age/FreshnessWindow)
	HeatWeight      float64
	FreshnessWeight float64
	// FreshnessWindow is the age at which the recency component reaches zero.
	FreshnessWindow time.Duration

	// DefaultTenantQuota limits the number of candidates per tenant; 0 means
	// unlimited. Per-tenant RegisterTenant overrides this.
	DefaultTenantQuota int

	// EnablePersistence turns on WAL + snapshot durability under WALDir.
	EnablePersistence bool
	// WALDir is the directory holding "wal.log" and "snapshot.gob".
	WALDir string
	// SnapshotEveryN forces a checkpoint after this many committed log records
	// since the last snapshot. 0 disables count-based checkpointing.
	SnapshotEveryN int
	// CheckpointMaxAge forces a checkpoint when the WAL's oldest uncheckpointed
	// record is older than this. 0 disables age-based checkpointing.
	CheckpointMaxAge time.Duration
}

// DefaultConfig returns the built-in configuration.
func DefaultConfig() Config {
	return Config{
		MaxCandidates:      10,
		ScanLimit:          5000,
		QueryTimeout:       50 * time.Millisecond,
		MaxInputRunes:      128,
		HalfLife:           24 * time.Hour,
		HeatWeight:         1.0,
		FreshnessWeight:    0.2,
		FreshnessWindow:    7 * 24 * time.Hour,
		DefaultTenantQuota: 0,
		EnablePersistence:  false,
		WALDir:             "",
		SnapshotEveryN:     10_000,
		CheckpointMaxAge:   time.Hour,
	}
}

// Validate checks the configuration and returns an error describing the first
// invalid field.
func (c Config) Validate() error {
	switch {
	case c.MaxCandidates <= 0:
		return errors.New("autocomplete: MaxCandidates must be > 0")
	case c.ScanLimit <= 0:
		return errors.New("autocomplete: ScanLimit must be > 0")
	case c.QueryTimeout <= 0:
		return errors.New("autocomplete: QueryTimeout must be > 0")
	case c.MaxInputRunes <= 0:
		return errors.New("autocomplete: MaxInputRunes must be > 0")
	case c.HalfLife <= 0:
		return errors.New("autocomplete: HalfLife must be > 0")
	case c.HeatWeight < 0 || c.FreshnessWeight < 0:
		return errors.New("autocomplete: weights must be >= 0")
	case c.HeatWeight == 0 && c.FreshnessWeight == 0:
		return errors.New("autocomplete: at least one ranking weight must be > 0")
	case c.FreshnessWeight > 0 && c.FreshnessWindow <= 0:
		return errors.New("autocomplete: FreshnessWindow must be > 0 when FreshnessWeight > 0")
	case c.DefaultTenantQuota < 0:
		return errors.New("autocomplete: DefaultTenantQuota must be >= 0")
	case c.EnablePersistence && c.WALDir == "":
		return errors.New("autocomplete: WALDir required when persistence enabled")
	case c.SnapshotEveryN < 0:
		return errors.New("autocomplete: SnapshotEveryN must be >= 0")
	}
	return nil
}
