# Self-check: TrailLog + SqliteLog (A1)

Slice: local sync `TrailLog` boundary. No Comb crate. No ObjectLog adapter.
No new long-lived actor.

## What landed

- `crates/pher/src/traillog.rs`: `Pos`, `TrailError`, `TrailLog`, `SqliteLog`.
- Daemon ingest assigns no seq. `SqliteLog::append` owns allocation.
- Head and floor live in `log_state` (`head`, `floor`). Head is written at
  append (and jsonl `seq` import). `MAX(events.seq)` is not the durable head.
- `Db` keeps deliveries/verdicts/vectors/forwarded/`matched_through` and
  `event_by_id`. It no longer inserts, ranges, or GC-deletes events.
- Tail, listen, `since`, and the follower page with `read` / `for_each_page`
  (`LOG_PAGE` = 256). They do not load the whole log into one `Vec` then trim.

## Connection ownership

Two rusqlite connections to `pher.db`:

1. `SqliteLog` (`Mutex<Connection>`) is the only writer of `events` and
   `log_state`. `BEGIN IMMEDIATE` on append/trim. `busy_timeout` 5s.
2. `Db` reads `events` for `event_by_id` and writes the other tables.

WAL makes the side index visible after commit. `SqliteLog` imports
`events.jsonl` and writes `head` from each line's `seq` field. Reopen never
sets `head` from `MAX(events.seq)`. Pre-A1 leftover event rows are adopted
only when creating the head row for the first time.

## Retention change (contiguous log)

Old GC: `DELETE FROM events WHERE ts < cutoff` (holes; a later old-ts
forwarded event could vanish while an earlier new-ts event stayed).

New: prefix walk in seq order. Stop at the first `envelope.ts >= cutoff`.
Floor is that prefix, never `MAX(deleted seq)` while a lower seq remains.
`seek_ts` is first seq `> floor` with `events.ts >= ts` (envelope ts, not
Comb `Frame.at`). `since` catch-up is then a seq range, so a later old-ts
event after the seek point is delivered.

## Limits

- Partition must be 0.
- `append([])` → `EmptyBatch`.
- `read` max must be `1..=4096`.
- Daemon follower/listen page size 256.

## Protocol still local-shaped (ObjectLog later)

- `listen`/`status`/`CursorLs` still speak a single `u64` seq, not a Pos
  vector. Fine while partition is always 0.
- Tails are still in-process `notify_tails` at ingest, plus paged backlog.
  Comb follow is not wired.
- Ingest still holds `Mutex<State>` across a local SQLite append (µs). No
  `block_on` under that lock. ObjectLog must append off the state lock.
- `deliveryId` is still `{sub}:{event.id}:{n}`. Follower gap replay can mint
  a new `n`. Deterministic ids are a later slice.
- Comb `read` is still unbounded and async; this crate does not depend on it.

## Checks run

```
CARGO_TARGET_DIR=/Users/trmd/Projects/honeybee/pheromone/repos/pheromone/target
cargo test -p pher --offline -- --test-threads=1
# 41 passed (bin pher unit tests; --lib is invalid, pher is a binary crate)
cargo clippy -p pher --offline --tests -- -W clippy::all
# clean on this slice; pre-existing unused fields in connector.rs
rustfmt crates/pher/src/{traillog,daemon,db,http,main}.rs
```

No shared daemon. No `~/.pheromone`. No Comb edits.

## Tests that pin the hazards

- two connections / two threads: contiguous unique seqs
- full trim + reopen: next seq is `head+1`, not 1
- empty batch + aborted batch: head and rows unchanged
- `read` max 0 / `MAX_READ+1` / partition 1
- out-of-order ts: prefix trim keeps a later old-ts event
- follower restart gap; pre-split catch-up; trimmed prefix jump; 80-event pages
- `event_by_id` after SqliteLog append; jsonl `seq` becomes head; wipe rows
  and reopen keeps head
