package autocomplete

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Engine is the concurrency-safe service entry point.
//
// Concurrency model:
//   - Query calls are lock-free against atomic.Pointer[Snapshot]; they always
//     observe one complete committed version and never block writes.
//   - All commits are serialized by wmu. Each commit writes+fsyncs the WAL
//     BEFORE publishing its in-memory snapshot, so a crash can never expose a
//     state that is not recoverable, and a failed write leaves the committed
//     version untouched.
type Engine struct {
	cfg     Config
	idx     *Index
	persist *persist
	clock   Clock
	scorer  *Scorer

	wmu      sync.Mutex // serializes all commits and Close
	closed   bool
	batchSeq uint64

	statsMu sync.RWMutex
	stats   map[string]*counters
}

type counters struct {
	updates         atomic.Int64
	deletes         atomic.Int64
	rejected        atomic.Int64
	batchesDone     atomic.Int64
	queries         atomic.Int64
	queriesTimedOut atomic.Int64
}

func (e *Engine) countersFor(tenant string) *counters {
	e.statsMu.RLock()
	c := e.stats[tenant]
	e.statsMu.RUnlock()
	if c != nil {
		return c
	}
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	if c = e.stats[tenant]; c == nil {
		c = &counters{}
		e.stats[tenant] = c
	}
	return c
}

// NewEngine builds an empty engine. When cfg.EnablePersistence is true it
// first recovers from cfg.WALDir, reproducing exactly the last committed
// state (including the committed-or-nothing batch semantics).
func NewEngine(cfg Config) (*Engine, error) {
	return NewEngineWithClock(cfg, SystemClock{})
}

// NewEngineWithClock is NewEngine with an injected time source (tests).
func NewEngineWithClock(cfg Config, clock Clock) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	e := &Engine{cfg: cfg, clock: clock, stats: map[string]*counters{}}
	e.scorer = NewScorer(cfg)

	initial := newSnapshot()
	if cfg.EnablePersistence {
		p, err := openPersist(cfg, clock)
		if err != nil {
			return nil, err
		}
		st, err := p.recover()
		if err != nil {
			p.close()
			return nil, err
		}
		p.adoptRecovery(st)
		e.persist = p
		initial = st.snapshot
		for id := range initial.tenants {
			e.countersFor(id)
		}
	}
	e.idx = newIndex(cfg, e.scorer, clock, initial)
	return e, nil
}

// RegisterTenant creates an isolated candidate space with its own quota
// (0 = use DefaultTenantQuota). It is durable when persistence is enabled.
func (e *Engine) RegisterTenant(ctx context.Context, id string, quota int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateTenant(id); err != nil {
		return err
	}
	if quota < 0 {
		return fmt.Errorf("%w: quota must be >= 0", ErrInvalidInput)
	}
	if quota == 0 {
		quota = e.cfg.DefaultTenantQuota
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.closed {
		return ErrClosed
	}
	if _, ok := e.idx.current().tenants[id]; ok {
		return fmt.Errorf("%w: tenant %q already registered", ErrBatchConflict, id)
	}
	if e.persist != nil {
		if err := e.persist.log(walRecord{Op: opTenantRegister, Tenant: id, Quota: quota}, true); err != nil {
			return err
		}
	}
	snap, err := e.idx.registerTenant(id, quota)
	if err != nil {
		return err
	}
	e.countersFor(id)
	return e.afterCommit(snap)
}

// Upsert adds or accumulates one candidate. Duplicate keys accumulate heat
// and use count (never overwrite); the call commits atomically.
func (e *Engine) Upsert(ctx context.Context, tenant string, item BatchItem) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.closed {
		return ErrClosed
	}
	entry, err := e.prepareEntry(tenant, item)
	if err != nil {
		e.reject(tenant, err)
		return err
	}
	if err := e.quotaPrecheck(tenant, []string{entry.Key}); err != nil {
		e.reject(tenant, err)
		return err
	}
	if e.persist != nil {
		if err := e.persist.log(entryToRecord(tenant, "", entry), true); err != nil {
			return err
		}
	}
	snap, err := e.idx.publish(mutation{tenant: tenant, upserts: []Entry{entry}})
	if err != nil {
		e.reject(tenant, err)
		return err
	}
	e.countersFor(tenant).updates.Add(1)
	return e.afterCommit(snap)
}

// Delete removes a candidate. Deleting a missing key is a successful no-op.
func (e *Engine) Delete(ctx context.Context, tenant, display string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := validateText(display, e.cfg.MaxInputRunes)
	if err != nil {
		return err
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.closed {
		return ErrClosed
	}
	if e.idx.current().tenant(tenant) == nil {
		return ErrUnknownTenant
	}
	if e.persist != nil {
		if err := e.persist.log(walRecord{Op: opDelete, Tenant: tenant, Key: key}, true); err != nil {
			return err
		}
	}
	snap, err := e.idx.publish(mutation{tenant: tenant, deletes: []string{key}})
	if err != nil {
		return err
	}
	e.countersFor(tenant).deletes.Add(1)
	return e.afterCommit(snap)
}

// BeginBatch opens a transactional import. Until Commit, none of its items are
// visible or counted against quota.
func (e *Engine) BeginBatch(ctx context.Context, tenant string) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.closed {
		return nil, ErrClosed
	}
	if e.idx.current().tenant(tenant) == nil {
		return nil, ErrUnknownTenant
	}
	e.batchSeq++
	id := fmt.Sprintf("b-%016x-%d", e.clock.Now().UnixNano(), e.batchSeq)
	return &Batch{id: id, tenant: tenant, state: batchActive}, nil
}

// AddToBatch appends one item (statically validated immediately; timestamps
// and default heat are assigned at commit time, so a batch reflects one
// committed instant). Repeated keys are kept and accumulated at commit.
func (e *Engine) AddToBatch(ctx context.Context, b *Batch, item BatchItem) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil {
		return ErrBatchConflict
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != batchActive {
		return ErrBatchConflict
	}
	if _, err := validateItem(b.tenant, item, e.cfg.MaxInputRunes); err != nil {
		e.reject(b.tenant, err)
		return err
	}
	b.items = append(b.items, item)
	return nil
}

// Import drains src into a fresh batch and commits it atomically. It returns
// the batch id and number of imported items. On source or validation error
// nothing is committed; partially read items are discarded with the batch.
func (e *Engine) Import(ctx context.Context, tenant string, src ImportSource) (string, int, error) {
	b, err := e.BeginBatch(ctx, tenant)
	if err != nil {
		return "", 0, err
	}
	for {
		if err := ctx.Err(); err != nil {
			e.abort(b)
			return "", 0, err
		}
		item, err := src.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				src.Close()
				break
			}
			src.Close()
			e.abort(b)
			return "", 0, err
		}
		if err := e.AddToBatch(ctx, b, item); err != nil {
			e.abort(b)
			return "", 0, err
		}
	}
	n, err := e.CommitBatch(ctx, b)
	return b.id, n, err
}

// CommitBatch publishes the whole batch in one atomic mutation after a single
// fsync'd commit record. On any failure the committed version is untouched.
func (e *Engine) CommitBatch(ctx context.Context, b *Batch) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if b == nil {
		return 0, ErrBatchConflict
	}
	// Reserve the batch for this commit under its own lock; concurrent adds
	// are rejected from here on without racing the items slice.
	b.mu.Lock()
	if b.state != batchActive {
		b.mu.Unlock()
		return 0, ErrBatchConflict
	}
	b.state = batchCommitting
	items := b.items
	b.mu.Unlock()

	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.closed {
		b.setState(batchAborted)
		return 0, ErrClosed
	}
	if e.idx.current().tenant(b.tenant) == nil {
		b.setState(batchAborted)
		return 0, ErrUnknownTenant
	}

	// Merge and validate everything up front; nothing is applied on failure.
	merged := make(map[string]Entry, len(items))
	order := make([]string, 0, len(items))
	now := e.clock.Now()
	for _, item := range items {
		en, err := e.prepareEntryLocked(b.tenant, item, now)
		if err != nil {
			b.setState(batchAborted)
			e.reject(b.tenant, err)
			return 0, err
		}
		if old, ok := merged[en.Key]; ok {
			en = accumulate(old, en)
		} else {
			order = append(order, en.Key)
		}
		merged[en.Key] = en
	}
	if err := e.quotaPrecheck(b.tenant, order); err != nil {
		b.setState(batchAborted)
		e.reject(b.tenant, err)
		return 0, err
	}

	if e.persist != nil {
		if err := e.persist.log(walRecord{Op: opBatchBegin, Tenant: b.tenant, BatchID: b.id}, false); err != nil {
			b.setState(batchAborted)
			return 0, err
		}
		for _, k := range order {
			en := merged[k]
			if err := e.persist.log(entryToRecord(b.tenant, b.id, en), false); err != nil {
				// No commit record will follow: recovery discards these rows.
				b.setState(batchAborted)
				return 0, err
			}
		}
		if err := e.persist.log(walRecord{Op: opBatchCommit, Tenant: b.tenant, BatchID: b.id}, true); err != nil {
			b.setState(batchAborted)
			return 0, err
		}
	}

	upserts := make([]Entry, 0, len(order))
	for _, k := range order {
		upserts = append(upserts, merged[k])
	}
	snap, err := e.idx.publish(mutation{tenant: b.tenant, upserts: upserts})
	if err != nil {
		// Cannot normally happen after prechecks; do not claim success.
		b.state = batchAborted
		e.reject(b.tenant, err)
		return 0, err
	}
	b.state = batchCommitted
	c := e.countersFor(b.tenant)
	c.updates.Add(int64(len(upserts)))
	c.batchesDone.Add(1)
	if err := e.afterCommit(snap); err != nil {
		return len(upserts), err
	}
	return len(upserts), nil
}

// AbortBatch discards a batch; it leaves no state anywhere (its WAL rows, if
// persistence ever wrote them, can only exist together with a commit record,
// and Commit writes that last).
func (e *Engine) AbortBatch(ctx context.Context, b *Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.abort(b)
}

func (e *Engine) abort(b *Batch) error {
	if b == nil {
		return ErrBatchNotFound
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != batchActive {
		return ErrBatchConflict
	}
	b.state = batchAborted
	b.items = nil
	return nil
}

func (b *Batch) setState(st batchState) {
	b.mu.Lock()
	b.state = st
	if st == batchAborted || st == batchCommitted {
		b.items = nil
	}
	b.mu.Unlock()
}

// Query returns candidates ranked for text within opts bounds. Matching is
// case/accent-width insensitive on normalized keys; the returned Entry.Display
// keeps the original surface form. A timeout yields ErrQueryTimeout with no
// partial results.
func (e *Engine) Query(ctx context.Context, tenant, text string, opts QueryOptions) ([]Candidate, QueryStats, error) {
	if err := ctx.Err(); err != nil {
		return nil, QueryStats{}, err
	}
	key, err := validateText(text, e.cfg.MaxInputRunes)
	if err != nil {
		return nil, QueryStats{}, err
	}
	snap := e.idx.current()
	if snap.tenant(tenant) == nil {
		return nil, QueryStats{Version: snap.version}, ErrUnknownTenant
	}
	if d, ok := ctx.Deadline(); ok {
		remaining := time.Until(d)
		if remaining <= 0 {
			return nil, QueryStats{Version: snap.version}, context.DeadlineExceeded
		}
		if opts.Timeout <= 0 || remaining < opts.Timeout {
			opts.Timeout = remaining
		}
	}
	res, stats, err := e.idx.Query(tenant, key, opts)
	c := e.countersFor(tenant)
	c.queries.Add(1)
	if err == ErrQueryTimeout {
		c.queriesTimedOut.Add(1)
	}
	return res, stats, err
}

// TenantStats returns counters for exactly one tenant; another tenant can
// neither read nor influence them.
func (e *Engine) TenantStats(ctx context.Context, tenant string) (TenantStats, error) {
	if err := ctx.Err(); err != nil {
		return TenantStats{}, err
	}
	snap := e.idx.current()
	ts := snap.tenant(tenant)
	if ts == nil {
		return TenantStats{}, ErrUnknownTenant
	}
	c := e.countersFor(tenant)
	return TenantStats{
		Candidates:      len(ts.entries),
		Updates:         c.updates.Load(),
		Deletes:         c.deletes.Load(),
		Rejected:        c.rejected.Load(),
		BatchesDone:     c.batchesDone.Load(),
		Queries:         c.queries.Load(),
		QueriesTimedOut: c.queriesTimedOut.Load(),
	}, nil
}

// Close flushes persistence and rejects further operations.
func (e *Engine) Close() error {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.closed {
		return ErrClosed
	}
	e.closed = true
	if e.persist != nil {
		return e.persist.close()
	}
	return nil
}

// ---- internals ----

func (e *Engine) afterCommit(snap *Snapshot) error {
	if e.persist == nil {
		return nil
	}
	return e.persist.maybeCheckpoint(snap)
}

func (e *Engine) reject(tenant string, err error) {
	if tenant != "" {
		e.countersFor(tenant).rejected.Add(1)
	}
}

func (e *Engine) prepareEntry(tenant string, item BatchItem) (Entry, error) {
	return e.prepareEntryLocked(tenant, item, e.clock.Now())
}

func validateItem(tenant string, item BatchItem, maxRunes int) (string, error) {
	key, err := validateText(item.Display, maxRunes)
	if err != nil {
		return "", err
	}
	if math.IsNaN(item.Heat) || math.IsInf(item.Heat, 0) || item.Heat < 0 {
		return "", fmt.Errorf("%w: heat must be a finite non-negative number", ErrInvalidInput)
	}
	if item.UseCount < 0 {
		return "", fmt.Errorf("%w: use count must be >= 0", ErrInvalidInput)
	}
	return key, nil
}

func (e *Engine) prepareEntryLocked(tenant string, item BatchItem, now time.Time) (Entry, error) {
	key, err := validateItem(tenant, item, e.cfg.MaxInputRunes)
	if err != nil {
		return Entry{}, err
	}
	heat, uses := item.Heat, item.UseCount
	if heat == 0 && uses == 0 {
		heat, uses = 1, 1 // a plain suggestion counts as one usage
	}
	last := item.LastUsedAt.UTC()
	if last.IsZero() {
		last = now
	}
	return Entry{
		Tenant:     tenant,
		Key:        key,
		Display:    item.Display,
		Heat:       heat,
		UseCount:   uses,
		CreatedAt:  now,
		LastUsedAt: last,
	}, nil
}

func (e *Engine) quotaPrecheck(tenant string, newKeys []string) error {
	ts := e.idx.current().tenant(tenant)
	if ts == nil {
		return ErrUnknownTenant
	}
	if ts.quota == 0 {
		return nil
	}
	added := 0
	for _, k := range newKeys {
		if _, ok := ts.entries[k]; !ok {
			added++
		}
	}
	if len(ts.entries)+added > ts.quota {
		return ErrTenantQuotaExceeded
	}
	return nil
}

func entryToRecord(tenant, batchID string, en Entry) walRecord {
	return walRecord{
		Op:         opUpsert,
		Tenant:     tenant,
		Key:        en.Key,
		Display:    en.Display,
		Heat:       en.Heat,
		UseCount:   en.UseCount,
		CreatedAt:  en.CreatedAt,
		LastUsedAt: en.LastUsedAt,
		BatchID:    batchID,
	}
}

func validateTenant(id string) error {
	if id == "" {
		return fmt.Errorf("%w: empty tenant id", ErrInvalidInput)
	}
	if RuneLen(id) > 256 {
		return fmt.Errorf("%w: tenant id too long", ErrTooLong)
	}
	for _, r := range id {
		if r < 0x21 || r == 0x7f {
			return fmt.Errorf("%w: tenant id contains control/whitespace", ErrInvalidInput)
		}
	}
	return nil
}

func validateText(s string, maxRunes int) (string, error) {
	if s == "" {
		return "", fmt.Errorf("%w: empty text", ErrInvalidInput)
	}
	k := Normalize(s)
	if k == "" {
		return "", fmt.Errorf("%w: text has no content after normalization", ErrInvalidInput)
	}
	if RuneLen(k) > maxRunes {
		return "", fmt.Errorf("%w: %d runes (limit %d)", ErrTooLong, RuneLen(k), maxRunes)
	}
	return k, nil
}
