package autocomplete

import "errors"

// Distinguishable sentinel errors. Callers can use errors.Is to classify a
// failure without parsing strings.
var (
	// ErrInvalidInput is returned when a query or candidate text is empty or
	// contains disallowed content after normalization.
	ErrInvalidInput = errors.New("autocomplete: invalid input")
	// ErrTooLong is returned when the normalized input exceeds the configured
	// maximum length.
	ErrTooLong = errors.New("autocomplete: input too long")
	// ErrUnknownTenant is returned when an operation references a tenant that
	// was never registered.
	ErrUnknownTenant = errors.New("autocomplete: unknown tenant")
	// ErrTenantQuotaExceeded is returned when an update would violate a
	// tenant's candidate quota.
	ErrTenantQuotaExceeded = errors.New("autocomplete: tenant quota exceeded")
	// ErrQueryTimeout is returned when a query cannot finish within its
	// configured timeout. A timeout never returns partial results.
	ErrQueryTimeout = errors.New("autocomplete: query timeout")
	// ErrBatchNotFound is returned when a batch id is unknown or already
	// completed.
	ErrBatchNotFound = errors.New("autocomplete: batch not found")
	// ErrBatchConflict is returned when a batch operation is applied in an
	// illegal state (e.g. committing twice).
	ErrBatchConflict = errors.New("autocomplete: batch conflict")
	// ErrCorrupt is returned when persisted data cannot be decoded or fails a
	// checksum check.
	ErrCorrupt = errors.New("autocomplete: corrupt persisted data")
	// ErrClosed is returned when the engine has been closed.
	ErrClosed = errors.New("autocomplete: engine closed")
)
