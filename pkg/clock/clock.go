// Package clock abstracts the time source used for heat decay and recency.
// Production uses Real; tests inject a Fake for deterministic decay checks.
package clock

import "time"

// Clock is the time source for the engine. All decay/recency decisions use
// Now(), so the decay baseline is always "now" from this interface.
type Clock interface {
	Now() time.Time
}

// Real uses the wall clock.
type Real struct{}

// Now returns the current wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// Fake is a manually controlled clock for tests.
type Fake struct {
	t time.Time
}

// NewFake returns a frozen clock set at t.
func NewFake(t time.Time) *Fake { return &Fake{t: t} }

// Now returns the frozen time.
func (f *Fake) Now() time.Time { return f.t }

// Set freezes the clock at t.
func (f *Fake) Set(t time.Time) { f.t = t }

// Advance moves the frozen clock forward by d.
func (f *Fake) Advance(d time.Duration) { f.t = f.t.Add(d) }
