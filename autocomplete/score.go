package autocomplete

import (
	"math"
	"time"
)

// Scorer computes deterministic ranking values. All time inputs are explicit;
// the scorer never reads the wall clock, so a fixed "now" yields a fixed
// ordering and repeated queries against the same snapshot are byte-identical.
type Scorer struct {
	halfLife        time.Duration
	heatWeight      float64
	freshnessWeight float64
	freshnessWindow time.Duration
}

// NewScorer builds a Scorer from a validated Config.
func NewScorer(cfg Config) *Scorer {
	return &Scorer{
		halfLife:        cfg.HalfLife,
		heatWeight:      cfg.HeatWeight,
		freshnessWeight: cfg.FreshnessWeight,
		freshnessWindow: cfg.FreshnessWindow,
	}
}

// Score returns the ranking value of e as observed at time now:
//
//	decayedHeat = e.Heat * 0.5^(age/HalfLife), age = now - e.LastUsedAt
//	freshness   = max(0, 1 - age/FreshnessWindow)
//	score       = HeatWeight*decayedHeat + FreshnessWeight*freshness
//
// A negative age (LastUsedAt in the future due to clock skew) is clamped to
// zero so such entries are not ranked above everything forever.
func (s *Scorer) Score(e Entry, now time.Time) float64 {
	age := now.Sub(e.LastUsedAt)
	if age < 0 {
		age = 0
	}
	heat := e.Heat * math.Pow(0.5, float64(age)/float64(s.halfLife))
	score := s.heatWeight * heat
	if s.freshnessWeight > 0 && s.freshnessWindow > 0 {
		f := 1.0 - float64(age)/float64(s.freshnessWindow)
		if f < 0 {
			f = 0
		}
		score += s.freshnessWeight * f
	}
	return score
}

// Less defines the total, deterministic ordering used for ranking:
//
//  1. higher score first,
//  2. then higher accumulated use count,
//  3. then earlier first-seen time,
//  4. then lexicographically smaller normalized key.
//
// Floating tie values are compared exactly; because every score derives from
// the same snapshot fields and one shared "now", equal inputs produce equal
// bits. There is therefore no randomness and no map-iteration dependence.
func (s *Scorer) Less(a Candidate, b Candidate) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	if a.Entry.UseCount != b.Entry.UseCount {
		return a.Entry.UseCount > b.Entry.UseCount
	}
	if !a.Entry.CreatedAt.Equal(b.Entry.CreatedAt) {
		return a.Entry.CreatedAt.Before(b.Entry.CreatedAt)
	}
	return a.Entry.Key < b.Entry.Key
}
