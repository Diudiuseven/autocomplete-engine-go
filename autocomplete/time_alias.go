package autocomplete

import "time"

// timeAlias wraps time.Time for gob snapshot encoding while keeping the
// persistence transport explicit.
type timeAlias struct{ time.Time }
