# Scale — the trail on object storage

**Status: design, 2026-08-21. Nothing here is implemented.** Direction decided: roll our own
WAL + object storage + CAS log; do not adopt a hosted log (S2 et al.) as the durability
layer. S3 and MinIO are the two first-class backends from day one.

## Why

Today one pherd is one process, one SQLite file, one critical section: `ingest_envelope`
assigns `next_seq`, writes SQLite, runs the cascade, and enqueues delivery under a single
`Mutex<State>` (`crates/pher/src/daemon.rs`). That is the right shape for the local trail
(µs acks, kill -9 safe, no infra) and it stays. It is the wrong shape for a hub that must
hold billions of events, survive a machine loss, be read by many consumers, and be replayed
arbitrarily far back:

- durability is one disk; retention is bounded by it;
- a trail has exactly one possible writer and no way to fence a second;
- ingest throughput equals matcher throughput because they are the same loop;
- cursors, outbox, and dedup windows are per-process files.

The fix is not a cluster protocol. It is the pattern S2, Cursor's Continuity, WarpStream and
SlateDB all converged on: **object storage is the only durable state; a manifest per log,
swapped by compare-and-swap, is the linearization point; servers are stateless caches.**
Everything above the log — envelope, language, cascade, sinks, bridges, grants — is
untouched. Cloud mode is the same `pherd` binary with a different `[store]`.

## Goals / non-goals

Goals: durable before ack (multi-AZ when the bucket is); infinite retention at object-store
cost; linear read scaling (N followers, N match workers); exact replay from any seq or ts;
any server can take over a trail after a crash with no operator action; the local pherd and
the mesh keep working unchanged; one test suite runs against local fs, MinIO and S3.

Non-goals: sub-millisecond ack on the cloud path (the local trail owns that); total order
across partitions of a partitioned trail; a general-purpose queue API (the log is internal,
the product surface is still subscriptions); multi-region active-active (one region per
trail; cross-region is a bridge).

## Two structural moves

### 1. Ingest → log → match as a consumer (P0, independent of object storage)

Split the critical section: ingest only appends to the `TrailLog`; the cascade runs in a
follower that reads the log from a cursor. This alone gives ingest decoupled from matcher
cost, N match workers, replay as "start a follower at seq N", and `meaning`/`judge` lagging
without blocking ingest. It is a prerequisite for everything below and improves the local
daemon today. Semantics preserved: per-trail order, at-least-once + `deliveryId` dedup.

### 2. The `TrailLog` trait with an object-store implementation

`Db::append_event / max_seq / events_after / events_since_ts / event_by_id / gc`
(`crates/pher/src/db.rs`) is already the implicit interface.

```rust
pub struct Pos { pub partition: u32, pub seq: u64 }   // single-partition trails: partition 0

pub trait TrailLog: Send + Sync {
    /// Append a batch atomically. Returns the Pos of the last record. Durable before return.
    fn append(&self, batch: &[Envelope]) -> Result<Pos>;
    fn head(&self) -> Result<Vec<Pos>>;                                  // one per partition
    fn read(&self, from: Pos, max: usize) -> Result<Vec<(Pos, Envelope)>>;
    fn seek_ts(&self, partition: u32, ts: &str) -> Result<Pos>;         // for `since`
    fn follow(&self, from: Pos) -> Result<Box<dyn Stream<Item = (Pos, Envelope)>>>;
    fn trim_before(&self, ts: &str) -> Result<()>;                       // retention
}
```

Implementations: `SqliteLog` (today's code, local), `ObjectLog` (below), `FsLog`
(`ObjectLog` over a local directory via `object_store::local` — the unit-test backend, also
usable as a single-machine durable log). `event_by_id` leaves the trait: the id → Pos index
is a side index (SQLite locally; a per-partition id index segment in the cloud, see
Compaction).

## ObjectLog

Crate: `object_store` (S3, GCS, Azure, local fs, in-memory behind one API; conditional
`put_opts` with `PutMode::Create` / `PutMode::Update(version)` covers S3 `If-None-Match` /
`If-Match` and MinIO's equivalents). No SDK per vendor.

### Layout

```
<bucket>/<tenant>/<trail>/
  manifest.json                         ← the CAS'd pointer. One per partition:
  p/<partition>/manifest.json             (single-partition trails: p/0/manifest.json only)
  p/<partition>/wal/<epoch>/<first_seq:020>.chunk
  p/<partition>/seg/<first_seq:020>-<last_seq:020>.seg
  p/<partition>/seg/<first_seq:020>-<last_seq:020>.idx    (id → seq, ts → seq)
  control/manifest.json                 ← subscriptions, cursors, grants, leases (CAS'd)
  control/deliveries/<partition>/…      ← delivery log, itself an ObjectLog
```

### Manifest (per partition)

```json
{
  "version": 1,
  "epoch": 17,                      // fencing. bumped on every leader change
  "leader": { "node": "hub-a", "lease_until": "2026-08-21T10:15:00Z" },
  "head_seq": 48213994,
  "wal": [ { "key": "wal/17/00000000000048210000.chunk", "first": 48210000, "last": 48213994,
             "ts_first": "…", "ts_last": "…", "bytes": 917304 } ],
  "segments": [ { "key": "seg/…-….seg", "idx": "seg/…-….idx", "first": 1, "last": 48209999,
                  "ts_first": "…", "ts_last": "…", "bytes": 4193941021 } ],
  "trim_before_seq": 0,
  "updated": "2026-08-21T10:14:58Z"
}
```

Small (KBs; the wal list is bounded by compaction), so readers poll it cheaply.

### Chunk / segment format

Length-prefixed frames: `u32 len | u64 seq | u64 ts_ms | u16 flags | bytes (canonical
envelope JSON)`, zstd-compressed per chunk, CRC32C trailer. Segments are concatenated,
re-compressed chunks plus an `.idx` with a sparse seq→offset table, ts→seq table, and an
id→seq hash index. Parquet is a later, optional export format for analytics, not the
primary format — we want random seek by seq without a columnar reader in the hot path.

### Append protocol (leader)

1. Ingest fronts forward appends to the partition leader (or become it — see Fencing).
2. Leader group-commits: collect appends for `commit_window` (default 10 ms) or
   `commit_bytes` (default 4 MiB), whichever first. Assign seqs `head+1..`.
3. `PUT wal/<epoch>/<first_seq>.chunk` with `PutMode::Create` (never overwrite).
4. `PUT manifest.json` with `PutMode::Update(etag)`: `head_seq`, appended wal entry, same
   epoch. On precondition failure: reload manifest; if epoch changed we are fenced → drop
   leadership, return `Fenced` to callers (they retry against the new leader); if only
   etag changed (compactor or lease renewal) → merge and retry the CAS.
5. Ack every append in the batch with its `Pos`. **Nothing is acked before step 4 succeeds.**

Orphaned chunks (PUT succeeded, CAS lost) are harmless: only chunks referenced by the
manifest exist logically; a sweeper deletes unreferenced `wal/` objects older than 1 h.

Throughput: the CAS rate per manifest is the ceiling (~100–300/s S3 Standard, several ×
on S3 Express One Zone, ~1k/s on MinIO/NVMe). Each CAS carries one group-commit; at 10 ms
windows and ~1 KB events that is millions of events/s per partition before partitioning.
Ack latency ≈ commit window + chunk PUT + manifest CAS: ~30–60 ms S3 Express, ~150–400 ms
S3 Standard, ~5–20 ms MinIO on the tailnet.

### Fencing and leadership

There is no leader election service. The manifest is the lock:

- To lead: read manifest; if `lease_until` is past (plus clock slack, 5 s) or the leader is
  us, CAS a new manifest with `epoch+1`, our node id, `lease_until = now + 30 s`. Success =
  we lead. Failure = someone else did; follow them.
- Renew by CAS every 10 s. Every append CAS also carries the epoch, so a stale leader that
  lost its lease **cannot** append even if it thinks it is alive: its CAS fails on epoch
  mismatch. Chunk keys include the epoch so two leaders can never collide on a key.
- Ingest fronts learn the leader from the manifest (cached; refreshed on `Fenced`).
- Clocks: only used for lease expiry, with slack. Correctness never depends on them; a
  slow clock only delays takeover.

### Read protocol (followers, match workers, bridges)

1. Conditional `GET manifest.json` with `If-None-Match: <etag>`; 304 ⇒ nothing new (<10 ms).
2. 200 ⇒ diff wal/segments against what we have; fetch new chunks; serve from NVMe cache.
3. Tail latency = poll interval. Default 250 ms; a WS/gossip "new manifest" hint from the
   leader drops it to ~1 RTT, but hints are unreliable by design — S3 is the truth.

Direct-from-bucket following is a first-class mode: a laptop with read-scoped credentials
for `<tenant>/<trail>/` can `pher follow` a cloud trail with **no pherd up in the cloud**.

### Compaction (one node, fenced the same way)

The leader also compacts (or a designated compactor that takes a `compactor` lease in the
same manifest). Merge wal chunks older than `compact_after` (default 5 min) or more than
`max_wal_chunks` (256) into a segment + idx; CAS the manifest replacing those wal entries.
Retention = bump `trim_before_seq`, delete segments fully below it after a grace period.
Infinite retention is just never trimming.

### Partitions

A trail starts with one partition. Partitioning is an operator action (`pher trail
partition <trail> --by subject --n 8`), effective at a seq boundary recorded in
`manifest.json` at the trail root. Routing: `hash(subject) % n`. Order is per partition;
correlation threads are per subject in practice, so this matches what users assume.
Cursors become a `Pos` vector. `listen --after N` on a single-partition trail is unchanged;
on a partitioned trail `--after` takes `p0:N,p1:M` (or a cursor name). This is the one
user-visible semantic change and it only appears when someone opts in.

## Control plane

`control/manifest.json`, same CAS discipline, holds subscriptions, named cursors, grants,
bridges, and leases. Low write rate, KB–MB scale. Every mutation is read–modify–CAS;
conflicts retry. Match workers take subscription-shard leases here (`shard i of n`, 30 s
lease); a worker that dies has its shard picked up within a lease. Workers each follow the
full log and evaluate only their shard — the subject trie makes the non-shard cost
negligible. If the control manifest ever gets hot, it moves to Postgres behind the same
trait; not before.

Deliveries are appended to `control/deliveries/` (an `ObjectLog`) so `why <delivery-id>`
works across restarts and nodes. Judge verdict cache and the forwarded-dedup window stay in
each worker's SQLite (cache semantics; loss is safe, cost-only).

## Tenants, grants, bridges

- Tenant = key prefix. Trails are per tenant. This answers ROADMAP open question 6.
- Grants stay the boundary enforcer at `/listen` and `/emit` on fronts. For bucket-direct
  follow, a grant is additionally materialised as a scoped credential (STS session policy
  on S3; MinIO policy) limited to `<tenant>/<trail>/` read. Emit never goes bucket-direct —
  only the leader writes.
- Bridges and `pher follow` are unchanged in semantics: a durable filtered pull that
  re-ingests locally with the same event id and `hops+1`. They gain a second transport
  (bucket-direct) next to `/listen`. The local daemon keeps `SqliteLog`; "stream a small
  subset down" is exactly a bridge with a subscription.

## Config

```toml
[store]
kind = "object"                 # "sqlite" (default, local) | "fs" | "object"
url = "s3://pher-prod/acme"     # or "s3://…?endpoint=http://minio.tail:9000" for MinIO
credentials = "hem:project/pheromone/s3"   # never inline
commit_window = "10ms"
commit_bytes = "4MiB"
lease = "30s"
poll = "250ms"
```

## Failure drills (must pass before "durable" is claimed)

Run the same drill set against `FsLog`, MinIO, and S3 (Standard and Express).

| # | Drill | Expected |
|---|---|---|
| 1 | kill -9 leader between chunk PUT and manifest CAS | chunk orphaned, no ack sent, no data visible; sweeper removes chunk |
| 2 | kill -9 leader after CAS before ack | client retries append with same event ids → admission dedup; no duplicate seq |
| 3 | two leaders (pause leader A past lease, start B, resume A) | A's next CAS fails on epoch; A returns `Fenced`; zero interleaving |
| 4 | compactor CAS races an append CAS | one retries; manifest never drops a wal entry that is not in a segment |
| 5 | reader with stale cache after compaction deleted a chunk | falls back to segment; no gap |
| 6 | bucket unavailable 60 s | appends block/fail fast (configurable), no ack; followers keep serving cache; recovery with no loss |
| 7 | clock skew +5 min on one node | takeover may be delayed/early but no double-leader (epoch CAS) |
| 8 | 1B-event replay from seq 1 | sequential segment reads, throughput ≥ 200 MB/s per follower, memory flat |
| 9 | trim while a follower is mid-read below `trim_before_seq` | follower gets `Trimmed{resume_at}`, same contract as today's retention-expired ack |
| 10 | MinIO single-node disk loss | documented as "MinIO durability is your erasure-set config", not ours |

Property test: a model-checked (or `loom`/`turmoil`-style) simulation of leaders, compactor,
and readers against an in-memory `object_store` with injected CAS failures, asserting
per-partition order, no ack without visibility, no visibility without manifest.

## Test matrix

| Backend | Where | Purpose |
|---|---|---|
| `object_store::memory` + fault injection | unit tests | protocol correctness |
| `FsLog` (local dir) | unit + integration | default CI backend, fast |
| MinIO (docker, single node) | CI job | real conditional-PUT semantics, tailnet deployment path |
| S3 Standard | nightly | latency/cost baseline |
| S3 Express One Zone | nightly | low-latency ack path |

Benchmarks to publish: append p50/p99 per backend at 1/10/100 producers; follower tail
latency; CAS/s ceiling per manifest; replay MB/s.

## Phasing

1. **P0 — ingest → log → follower split in-process.** Still SQLite, still one binary.
   Tests green, benchmarks unchanged or better.
2. **P1 — `TrailLog` trait; `SqliteLog`; `FsLog`/`ObjectLog`** with single leader, group
   commit, manifest CAS, reader, sweeper. Drills 1–3, 6 on memory + fs + MinIO.
3. **P2 — compaction, retention, id/ts index; drills 4, 5, 8, 9.** S3 nightly.
4. **P3 — stateless cloud pherd:** control manifest, shard leases, delivery log, leader
   takeover; `[store] kind = "object"` end to end on MinIO and S3.
5. **P4 — bucket-direct follow + scoped credentials; tenant provisioning; partitions.**

## Open questions

1. Frame format: keep canonical JSON in frames (debuggable, compresses ~5×) vs a binary
   envelope encoding. Leaning JSON + zstd; revisit if CPU on replay dominates.
2. Commit-window defaults per backend (10 ms is right for Express/MinIO; S3 Standard may
   want 50 ms to amortise the ~100 ms CAS).
3. Whether tier-3 vectors for the cloud trail live in a per-worker local index rebuilt from
   the log, or in their own compacted segment alongside the data. Leaning: rebuilt locally,
   bounded by the `novel` window; archive-scale semantic search is a different product.
4. Hosted log adapter (S2-style) behind `TrailLog`: cheap to add later if a customer
   insists; not on the path.

## Sources

- Cursor, *Git at any scale* (Continuity): WAL entries in S3, CAS on index, stateless NVMe
  cache, any-node-primary — https://cursor.com/blog/git-at-any-scale
- S2 architecture docs (chunks → segments on S3, ack-after-durable, fencing tokens) —
  https://s2.dev/docs/platform/architecture
- AWS S3 conditional writes (`If-None-Match` Aug 2024, `If-Match` Nov 2024); MinIO supports
  both.
- `object_store` crate: `put_opts` with `PutMode::Create` / `PutMode::Update`.
