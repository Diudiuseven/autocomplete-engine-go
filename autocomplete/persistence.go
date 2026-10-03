package autocomplete

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const snapshotFileName = "snapshot.gob"

// snapshotBlob is the on-disk transport form of a Snapshot. Only stable data
// is stored; derived caches (sortedKeys) and stats that describe uptime are
// rebuilt on load.
type snapshotBlob struct {
	Version uint64
	LastSeq uint64
	Tenants []snapshotTenantBlob
}

type snapshotTenantBlob struct {
	ID      string
	Quota   int
	Entries []snapshotEntryBlob
}

type snapshotEntryBlob struct {
	Key        string
	Display    string
	Heat       float64
	UseCount   int64
	CreatedAt  gobTime
	LastUsedAt gobTime
}

// gobTime is time.Time registered for gob transport.
type gobTime = timeAlias

// recoveryState is the materialized result of replaying persistence.
type recoveryState struct {
	snapshot     *Snapshot
	opsSinceSnap int
	// lastSeq is the highest record sequence seen (snapshot + WAL).
	lastSeq uint64
}

// persist coordinates snapshot files and the append-only WAL. Every mutation
// is durably logged BEFORE it is published in memory, so recovery always
// reaches at least the last committed state, and commit/fsync ordering plus
// batch boundaries guarantee it never applies uncommitted data.
type persist struct {
	dir   string
	wal   *WAL
	cfg   Config
	clock Clock

	mu            sync.Mutex
	opsSinceSnap  int
	lastSeq       uint64
	oldestRecTime time.Time
}

func openPersist(cfg Config, clock Clock) (*persist, error) {
	w, err := openWAL(cfg.WALDir)
	if err != nil {
		return nil, err
	}
	return &persist{dir: cfg.WALDir, wal: w, cfg: cfg, clock: clock}, nil
}

// adoptRecovery continues sequence numbering above the replayed high-water
// mark and remembers how many records the current snapshot still needs.
func (p *persist) adoptRecovery(st recoveryState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastSeq = st.lastSeq
	p.opsSinceSnap = st.opsSinceSnap
	p.wal.setHighWater(st.lastSeq)
}

// shouldCheckpoint reports whether the post-write state warrants a checkpoint.
func (p *persist) shouldCheckpointLocked() bool {
	if n := p.cfg.SnapshotEveryN; n > 0 && p.opsSinceSnap >= n {
		return true
	}
	if d := p.cfg.CheckpointMaxAge; d > 0 && !p.oldestRecTime.IsZero() &&
		p.clock.Now().Sub(p.oldestRecTime) >= d {
		return true
	}
	return false
}

// recover loads the newest valid snapshot, then replays the WAL once.
// Idempotency across an interrupted checkpoint is provided by record sequence
// numbers: every record with Seq <= snapshot.LastSeq is skipped. Batch
// records without a following opBatchCommit are staged and discarded, so an
// unfinished import never materializes partially.
func (p *persist) recover() (recoveryState, error) {
	st := recoveryState{snapshot: newSnapshot()}

	blob, snapLastSeq, haveSnap, err := loadSnapshot(p.dir)
	if err != nil {
		return st, err
	}
	if haveSnap {
		s := newSnapshot()
		s.version = blob.Version
		for _, tb := range blob.Tenants {
			ts := &tenantSnap{entries: make(map[string]Entry, len(tb.Entries)), quota: tb.Quota}
			for _, eb := range tb.Entries {
				ts.entries[eb.Key] = Entry{
					Tenant:     tb.ID,
					Key:        eb.Key,
					Display:    eb.Display,
					Heat:       eb.Heat,
					UseCount:   eb.UseCount,
					CreatedAt:  eb.CreatedAt.Time,
					LastUsedAt: eb.LastUsedAt.Time,
				}
			}
			ts.rebuildKeys()
			ts.stats.Candidates = len(ts.entries)
			s.tenants[tb.ID] = ts
		}
		st.snapshot = s
		st.lastSeq = snapLastSeq
	}

	f, err := os.Open(filepath.Join(p.dir, walFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return st, nil
		}
		return st, fmt.Errorf("autocomplete: read wal: %w", err)
	}
	defer f.Close()

	// staged[batchID] -> entries not yet committed
	staged := map[string]map[string]Entry{}
	batchTenant := map[string]string{}
	ops := 0
	_, err = scanWAL(f, func(rec walRecord) error {
		if rec.Seq != 0 && rec.Seq <= st.lastSeq {
			return nil // already contained in the snapshot; skip, never reapply
		}
		if rec.Seq > st.lastSeq {
			st.lastSeq = rec.Seq
		}
		switch rec.Op {
		case opTenantRegister:
			if _, ok := st.snapshot.tenants[rec.Tenant]; !ok {
				ts := &tenantSnap{entries: map[string]Entry{}, sortedKeys: []string{}, quota: rec.Quota}
				ts.stats.Candidates = 0
				st.snapshot.tenants[rec.Tenant] = ts
				st.snapshot.version++
			}
			ops++
		case opUpsert:
			e := recordToEntry(rec)
			if rec.BatchID != "" {
				stageUpsert(staged, batchTenant, rec.BatchID, rec.Tenant, e)
			} else {
				applyUpsert(st.snapshot, rec.Tenant, e)
				st.snapshot.version++
			}
			ops++
		case opDelete:
			if ts := st.snapshot.tenant(rec.Tenant); ts != nil {
				if _, ok := ts.entries[rec.Key]; ok {
					delete(ts.entries, rec.Key)
					ts.stats.Candidates--
				}
				ts.rebuildKeys()
			}
			st.snapshot.version++
			ops++
		case opBatchBegin:
			if _, ok := staged[rec.BatchID]; !ok {
				staged[rec.BatchID] = map[string]Entry{}
				batchTenant[rec.BatchID] = rec.Tenant
			}
		case opBatchCommit:
			tenant := batchTenant[rec.BatchID]
			for _, e := range staged[rec.BatchID] {
				applyUpsert(st.snapshot, tenant, e)
			}
			delete(staged, rec.BatchID)
			delete(batchTenant, rec.BatchID)
			st.snapshot.version++
		case opCheckpoint:
			// informational; snapshot itself carries LastSeq.
		}
		return nil
	})
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return st, err
	}
	// Uncommitted staged batches are intentionally dropped here.
	st.opsSinceSnap = ops
	return st, nil
}

func applyUpsert(s *Snapshot, tenant string, ne Entry) {
	ts := s.tenant(tenant)
	if ts == nil {
		ts = ensureTenant(s, tenant, 0)
	}
	if old, ok := ts.entries[ne.Key]; ok {
		ne = accumulate(old, ne)
	}
	ts.entries[ne.Key] = ne
	ts.rebuildKeys()
	ts.stats.Candidates = len(ts.entries)
}

func stageUpsert(staged map[string]map[string]Entry, batchTenant map[string]string, batchID, tenant string, e Entry) {
	m, ok := staged[batchID]
	if !ok {
		m = map[string]Entry{}
		staged[batchID] = m
		batchTenant[batchID] = tenant
	}
	if old, ok := m[e.Key]; ok {
		e = accumulate(old, e)
	}
	m[e.Key] = e
}

func recordToEntry(rec walRecord) Entry {
	return Entry{
		Tenant:     rec.Tenant,
		Key:        rec.Key,
		Display:    rec.Display,
		Heat:       rec.Heat,
		UseCount:   rec.UseCount,
		CreatedAt:  rec.CreatedAt,
		LastUsedAt: rec.LastUsedAt,
	}
}

// checkpoint persists s atomically: write temp file -> fsync -> rename ->
// fsync directory -> truncate WAL. A crash at any point leaves either the old
// snapshot+WAL or the new snapshot plus a WAL suffix that is skipped by Seq.
func (p *persist) checkpoint(s *Snapshot) error {
	blob := snapshotBlob{Version: s.version}
	for id, ts := range s.tenants {
		tb := snapshotTenantBlob{ID: id, Quota: ts.quota}
		for _, e := range ts.entries {
			tb.Entries = append(tb.Entries, snapshotEntryBlob{
				Key:        e.Key,
				Display:    e.Display,
				Heat:       e.Heat,
				UseCount:   e.UseCount,
				CreatedAt:  gobTime{e.CreatedAt},
				LastUsedAt: gobTime{e.LastUsedAt},
			})
		}
		blob.Tenants = append(blob.Tenants, tb)
	}
	// Capture the durable high-water sequence.
	p.mu.Lock()
	blob.LastSeq = p.lastSeq
	p.mu.Unlock()

	tmp := filepath.Join(p.dir, snapshotFileName+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("autocomplete: snapshot create: %w", err)
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(blob); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(p.dir, snapshotFileName)); err != nil {
		return fmt.Errorf("autocomplete: snapshot rename: %w", err)
	}
	if err := syncDir(p.dir); err != nil {
		return err
	}
	return p.wal.truncate()
}

func (p *persist) log(rec walRecord, fsync bool) error {
	if err := p.wal.append(rec, fsync); err != nil {
		return err
	}
	p.mu.Lock()
	if p.opsSinceSnap == 0 {
		p.oldestRecTime = p.clock.Now()
	}
	p.opsSinceSnap++
	if rec.Seq > p.lastSeq {
		p.lastSeq = rec.Seq
	}
	p.mu.Unlock()
	return nil
}

// maybeCheckpoint performs a snapshot+truncate when the configured thresholds
// are met. Errors here never fail the already-committed write; they are
// returned to the caller only for observability and the next write retries.
func (p *persist) maybeCheckpoint(s *Snapshot) error {
	p.mu.Lock()
	need := p.shouldCheckpointLocked()
	p.mu.Unlock()
	if !need {
		return nil
	}
	if err := p.checkpoint(s); err != nil {
		return err
	}
	p.mu.Lock()
	p.opsSinceSnap = 0
	p.oldestRecTime = time.Time{}
	p.mu.Unlock()
	return nil
}

func (p *persist) close() error { return p.wal.close() }

func loadSnapshot(dir string) (snapshotBlob, uint64, bool, error) {
	f, err := os.Open(filepath.Join(dir, snapshotFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return snapshotBlob{}, 0, false, nil
		}
		return snapshotBlob{}, 0, false, err
	}
	defer f.Close()
	var blob snapshotBlob
	if err := gob.NewDecoder(f).Decode(&blob); err != nil {
		return snapshotBlob{}, 0, false, fmt.Errorf("%w: snapshot decode: %v", ErrCorrupt, err)
	}
	return blob, blob.LastSeq, true, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
