# Scale — the trail at cloud scale is built on Comb

**Status: 2026-08-23. The object-storage log (WAL chunks + CAS'd manifest, fencing,
compaction, partitions, drills, MinIO/S3 certification) is owned by the Comb project**
— `honeybee/comb`, spec `comb-specification-v0_3.md`, §8 *Comb Log*, §13 *Pheromone
integration*, §23 *Phases*. Pheromone is Comb Log's first consumer and conformance workload.
This document is only what Pheromone itself owes and keeps.

## The boundary

```
pher (envelope, language, cascade, sinks, bridges, grants, cursors, control state)
  │ TrailLog trait  (Comb spec §8.2 — Pos{partition, seq}; append/head/read/seek_ts/follow/trim_before)
  ▼
SqliteLog (local, today's db.rs)   FsLog / ObjectLog (Comb Log over local dir / MinIO / S3)
```

Comb Core knows nothing about envelopes or subscriptions; Pheromone knows nothing about
chunks, manifests or leases. A need that would require a Core change means Pheromone is
redesigned, not Comb (Comb README). Dependency is one-directional: Pheromone is built on
Comb Log; Comb uses Pheromone only as a non-authoritative change-hint channel (§7.5a.6).

## What Pheromone owes: Phase A — the structural split (Comb §8.3, §23.2)

**Done 2026-08-23 (ingest/follower split).** `daemon.rs` no longer matches at ingest:

```
ingest → append (seq, SQLite, tails, nudge) → ack {id, seq}
follower thread: follow_step(32) from `matched_through` → cascade → delivery → commit position
```

- `matched_through` is a `meta` row in `pher.db`, committed after each follower batch. A
  crash between append and commit replays the gap on restart — at-least-once, nothing
  lost; `deliveryId`s of replayed deliveries get a fresh counter (same event id).
- Pre-split databases have no marker and start caught up (those events were matched
  synchronously when they were ingested).
- The `emit` ack dropped `deliveries` (it was already partial: tiers 3–4 and shaping
  windows returned 0). `pher status` reports `matchedThrough` next to `nextSeq`.
- Lock discipline: the follower holds the state lock for at most one batch of 32
  cascades; emitters interleave. Not yet parallel matching — that needs shard leases
  (Phase C/§8.13) and is unnecessary until one follower saturates.

Remaining for Phase A: lift `Db::append_event/max_seq/events_after/events_since_ts/gc` into `TrailLog` as
`SqliteLog`; `event_by_id` becomes a side index (SQLite locally; Comb `.idx` segments in
the cloud).

## What stays Pheromone-owned above the log (Comb §8.13, §13.5)

- Subscriptions, named cursors, grants, bridges, match-worker shard leases → Pheromone
  control manifest (a Comb ref, CAS'd).
- Deliveries → their own Comb Log so `why <delivery-id>` survives process/machine loss.
- Judge verdict cache, forwarded-dedup window, tier-3 vector window → local, rebuildable,
  cost-only.
- Grants enforced at `/emit` and `/listen`; a read grant may materialise as a prefix-scoped
  short-lived credential for bucket-direct `pher follow` (§13.4). Clients never write to the
  bucket; only the leader appends.
- Bridges keep their semantics (same event id, `hops+1`, admission dedup, cursor resume) and
  gain bucket-direct as a second transport next to `/listen`.

## Config (Comb §13.1–13.2)

```toml
[store]
kind = "sqlite"      # default, local trail — unchanged

[store]
kind = "object"      # cloud trail; MinIO via endpoint config
url = "s3://pher-prod/acme"
credentials = "hem:project/pheromone/s3"
commit_window = "10ms"
commit_bytes = "4MiB"
lease = "30s"
poll = "250ms"
```

## One user-visible semantic change

Partitioned trails (opt-in, Comb §8.12) order per partition; cursors become a `Pos`
vector and `listen --after` takes `p0:N,p1:M` or a cursor name. Single-partition trails —
every local trail and every cloud trail until an operator partitions it — keep today's
`--after N`.

## Sequencing

Comb phases A → B → C (§23). A is Pheromone work and can start now. C's exit criterion is
"Pheromone cloud mode runs end to end on MinIO and S3; no acknowledged event lost in the
required drills; 1B-event replay demonstrated; local mode remains available."
