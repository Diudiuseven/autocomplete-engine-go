# Autocomplete Engine (Go)

A tenant-aware input-association ("type-ahead") service. It returns ranked
candidates while the user types, supports both prefix and in-word matching,
and ranks by a blend of historical popularity (with explicit time decay) and
freshness (time since last use). Candidates arrive through bulk imports
(local file or in-memory pipe) and continuous incremental upserts/deletes.

The implementation is a single package, `autocomplete`; see the package docs
and tests for executable specifications.

## Quick start

```go
cfg := autocomplete.DefaultConfig()
cfg.EnablePersistence = true
cfg.WALDir = "./data"

eng, err := autocomplete.NewEngine(cfg)            // recovers last committed state
if err != nil { log.Fatal(err) }
defer eng.Close()

ctx := context.Background()
_ = eng.RegisterTenant(ctx, "shop-us", 0)          // 0 = unlimited quota
_ = eng.Upsert(ctx, "shop-us", autocomplete.BatchItem{Display: "Espresso"})

cands, stats, err := eng.Query(ctx, "shop-us", "es", autocomplete.QueryOptions{})
for _, c := range cands {
    fmt.Println(c.Entry.Display, c.Score, c.Match) // display form preserved
}
```

Bulk import from a local TSV file:

```go
src, _ := autocomplete.NewFileSource("terms.tsv")
id, n, err := eng.Import(ctx, "shop-us", src)      // one atomic commit
```

…or from an in-memory pipe / slice:

```go
eng.Import(ctx, "shop-us", autocomplete.NewChannelSource(ch))
eng.Import(ctx, "shop-us", autocomplete.NewMemorySource(items))
```

## Text normalization (one rule for everything)

Matching identity is the normalized form; the original string is kept on
`Entry.Display` and is what callers render. The pipeline (`Normalize`) is:

1. **NFKC** — compatibility decomposition + canonical composition. Unifies
   full-width/half-width (`Ａ`→`A`, `，`→`,`, `ﾊ`→`ハ`) and compatibility
   ligatures (`ﬁ`→`fi`).
2. **Unicode full case folding** (`cases.Fold`) — case-insensitive in the
   Unicode sense (`ß`↔`ss`), not a naïve ASCII lower.
3. **NFC** — canonical recomposition, so precomposed `é` and `e`+U+0301 are
   the same key.
4. Trim outer Unicode whitespace; collapse interior whitespace runs to one
   ASCII space.

Matching is therefore case-, width-, and combining-form-insensitive, while
the displayed term keeps the exact supplied surface form.

### Input rejection (distinguishable causes)

| Cause | Error |
|---|---|
| empty/whitespace-only, or empty after normalization | `ErrInvalidInput` |
| normalized rune length > `MaxInputRunes` | `ErrTooLong` |
| negative / NaN / ±Inf heat, negative use count | `ErrInvalidInput` |
| empty or malformed tenant id | `ErrInvalidInput` |
| tenant not registered (read or write) | `ErrUnknownTenant` |
| update would exceed per-tenant quota | `ErrTenantQuotaExceeded` |
| query exceeds its timeout | `ErrQueryTimeout` |
| persisted data structurally corrupt | `ErrCorrupt` |
| use after `Close` | `ErrClosed` |

All are sentinel errors; classify with `errors.Is`.

## Matching and ranking semantics

* **Match types.** A normalized key starting with the normalized query is
  `MatchPrefix`; any other containment is `MatchInfix`. Both are returned in
  one result set; the `Match` field distinguishes them.
* **Determinism.** The index stores each tenant's keys in sorted order and a
  query takes **one** logical timestamp from the engine `Clock`; every
  candidate is scored at that same instant. Ranking is a **total order**:

  1. higher score,
  2. then higher accumulated use count,
  3. then earlier first-seen (`CreatedAt`),
  4. then lexicographic normalized key.

  Consequently the same data + same config + same logical time always yield
  the identical byte-for-byte ordering — there is no map-iteration randomness.

* **Score.** Let `age = now − LastUsedAt` (negative ages from clock skew are
  clamped to 0):

  ```
  decayedHeat = Heat · 0.5^(age / HalfLife)
  freshness   = max(0, 1 − age / FreshnessWindow)
  score       = HeatWeight·decayedHeat + FreshnessWeight·freshness
  ```

* **Explicit time sources / decay basis.** Stored `Heat` is the *undecayed
  accumulated mass* and never mutates as wall-clock advances; decay is purely
  a read-time projection. The decay reference is the entry's fixed
  `LastUsedAt`; the observation point is the query instant from the
  configurable `Clock` (`SystemClock` in production, a controllable fake in
  tests). A query's *timeout* uses the real wall clock regardless of the
  injected clock, because timeout protects real latency.

## Consistency and concurrency boundary

* Reads (`Query`, `TenantStats`) are **lock-free**: they dereference an
  `atomic.Pointer[Snapshot]`. A `Snapshot` is an immutable whole-index
  version.
* Every update clones the current snapshot, mutates the private clone, and
  publishes it with one atomic swap. Therefore a query sees exactly one
  complete committed version: it can never observe a half-written map, a
  partially applied batch, or field-level mixing of an entry.
* Commits are serialized by a single write mutex, so concurrent upserts to the
  same key cannot lose accumulation. A failed update (quota, bad input, WAL
  error) discards its clone; the published version is untouched.
* **Durability ordering.** With persistence enabled, each mutation is
  written+`fsync`ed to the WAL *before* the in-memory snapshot is published.
* Slow queries run on an old immutable snapshot and never block or delay
  writes, and writes never make a running query observe a moving target.
* Memory per query is bounded: at most `ScanLimit` entries are inspected and
  at most `MaxCandidates` returned; no query allocates proportional to the
  whole index.

### Batch (import) atomicity

A batch is logged as `BEGIN`, `UPSERT*`, `COMMIT` (only the commit is
`fsync`-forced). Nothing is visible until commit; the commit applies the whole
deduplicated set in one mutation. Repeated keys within one batch accumulate.
On abort/error the batch leaves no in-memory state; on crash the uncommitted
WAL rows (no terminating COMMIT) are discarded during recovery.

## Persistence and recovery

Layout under `WALDir`:

* `wal.log` — append-only length-prefixed frames, each CRC-32 checksummed and
  carrying a monotonic sequence number.
* `snapshot.gob` — materialized snapshot plus the WAL high-water sequence it
  contains.

Recovery rules:

1. Load the newest valid `snapshot.gob` (a bad snapshot is `ErrCorrupt`, never
   silently ignored).
2. Replay `wal.log` from the beginning, **skipping any record with
   `Seq ≤ snapshot.LastSeq`** — so an interrupted checkpoint never causes
   double application or skipped records.
3. Batch rows without a following COMMIT are staged then dropped, so an
   unfinished import leaves no partial data.
4. A torn/partial trailing frame (the usual crash tail: short frame, failed
   CRC, undecodable gob, or implausible length reaching EOF) terminates replay
   as the log end. The same kind of damage *followed by intact frames* is a
   hard `ErrCorrupt`.

Checkpointing writes a temp file, fsyncs, atomically renames it, fsyncs the
directory, and only then truncates the WAL; sequence numbers keep increasing
across truncations. Restart therefore reproduces **exactly the last committed
state**, never more and never less.

## Tenancy, quotas, and statistics

Tenants are explicitly registered (`RegisterTenant`) with an optional quota
(max distinct candidates; `0` = unlimited, inherits `DefaultTenantQuota`).
Candidate maps, sorted keys, and `TenantStats` counters live entirely inside
the tenant's own snapshot section:

`Candidates, Updates, Deletes, Rejected, BatchesDone, Queries,
QueriesTimedOut`.

Reads and writes against an unknown tenant are rejected (`ErrUnknownTenant`),
so tenants can neither see each other's data nor share counters or quota
headroom.

## Query limits and timeout

`QueryOptions{MaxCandidates, ScanLimit, Timeout}` override the config for one
call (zero = use config). Scans check the real deadline periodically. On
timeout the call returns `ErrQueryTimeout` with **no** candidate list — a
deterministic "no result" rather than an unsorted/partial list — and
`QueriesTimedOut` is incremented. A `context.Context` deadline shorter than
the configured timeout bounds the call as well.

## Configuration reference (`DefaultConfig`)

| Field | Default | Meaning |
|---|---|---|
| `MaxCandidates` | `10` | Maximum candidates returned per query. |
| `ScanLimit` | `5000` | Max entries inspected per query (latency/memory bound). |
| `QueryTimeout` | `50ms` | Per-query wall-clock budget; timeout → `ErrQueryTimeout`. |
| `MaxInputRunes` | `128` | Max normalized rune length for queries and terms. |
| `HalfLife` | `24h` | Heat halves per interval: `Heat·0.5^(age/HalfLife)`. |
| `HeatWeight` | `1.0` | Weight of decayed-heat in the score. |
| `FreshnessWeight` | `0.2` | Weight of recency in the score. |
| `FreshnessWindow` | `7d` | Age at which the freshness component reaches 0. |
| `DefaultTenantQuota` | `0` | Per-tenant candidate cap when registration omits one (0 = unlimited). |
| `EnablePersistence` | `false` | Enable WAL + snapshot durability. |
| `WALDir` | `""` | Directory for `wal.log` / `snapshot.gob` (required if enabled). |
| `SnapshotEveryN` | `10000` | Checkpoint after this many new log records (0 = off). |
| `CheckpointMaxAge` | `1h` | Reserved age-based checkpoint knob (0 = off). |

`Config.Validate` enforces: positive limits/timeout/half-life; non-negative
weights with at least one positive; `FreshnessWindow > 0` when freshness is
weighted; non-negative quota; and `WALDir` when persistence is enabled.

## Import file format

UTF-8, one candidate per line, tab-separated fields; blank lines and `#`
comments are ignored:

```
# display [heat [useCount [lastUsedRFC3339]]]
Espresso	5.0	12	2025-09-01T10:00:00Z
Cappuccino	3
Latte
```

Malformed numeric/timestamp fields make the source return an error wrapping
`ErrInvalidInput`; the enclosing import then commits nothing.

## Tests

```sh
go test -race ./...
```

Coverage includes: normalization equivalence; invalid/oversized inputs;
prefix+infix matching and display preservation; dedup accumulation (single
upsert path, batch path, file/channel/memory sources); deterministic ordering
across repeated and cross-engine queries; heat-decay/freshness re-ranking with
an injected clock; concurrent read/write stress and lost-update checks under
`-race`; per-tenant isolation, stats, and quotas; scan caps and query timeout
(no partial results); restart recovery of committed state; dropping of
unfinished batches; no-double-apply across a checkpoint; torn-tail tolerance
vs. hard mid-file corruption; and idempotent deletes.
