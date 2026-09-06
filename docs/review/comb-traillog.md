# Self-check: TrailLog + SqliteLog (A1)

Slice: local sync `TrailLog` boundary. No Comb crate. No ObjectLog adapter.
No new long-lived actor.

## What landed

- `crates/pher/src/traillog.rs`: `Pos`, `TrailError`, `TrailLog`, `SqliteLog`.
- Daemon ingest assigns no seq. `SqliteLog::append` owns allocation.
- Head and floor live in `log_state`. Head is written at append (and jsonl
  `seq` import). Reopen never sets head from `MAX(events.seq)`.
- `Db` keeps deliveries/verdicts/vectors/forwarded/`matched_through` and
  `event_by_id`.
- Writer pragma: `synchronous=FULL` (SQLite durability mode 2). Not a
  hardware power-cut test.

## Connection / seed

Two rusqlite connections to `pher.db`. SqliteLog is the only writer of
`events` and `log_state`. Seed is one `BEGIN IMMEDIATE` (`INSERT OR IGNORE`
head/floor). Concurrent first opens cannot lower a persisted head. Leftover
pre-A1 rows are adopted only when head is still 0 in that transaction.

## Retention vs since

Prefix GC may keep a later old-ts event behind a newer one (contiguous
floor). `since` is not a seq range: `seek_ts` finds the first candidate,
then every later paged envelope must still have `envelope.ts >= cutoff`.
Tested with old/new/old/new.

## Catch-up bounding

`read` pages at most `LOG_PAGE` (256). `trim_before` uses `LIMIT 1` for the
first retained seq. Listen catch-up does not enqueue history on the live
channel. The consumer pages after attach; live tails/listeners use
`sync_channel(256)` and `try_send`. Full drops that consumer and removes
the matcher subscription. Catch-up trim/corruption after ack is an error
(disconnect/resume), not silent omission. `since` replay and tail
backlog also fail closed on corrupt/trim rather than skipping. Ack does
not include an `evaluate()` estimate as `replayed`. `after = u64::MAX`
does not overflow. No bounded send waits under `Mutex<State>`.

Catch-up applies matching, limits, and shaping once in sequence order.
The follower does not deliver seqs `<=` the captured head to that
listener (catch-up owns that range, including follower backlog not yet
processed at attach). Live matches above the captured head wait in a
`LIVE_QUEUE`-capped hold until catch-up finishes, so a later live event
cannot consume `limit` or arm shaping before history. `last_sent` is
output dedup only; it is not the limit/shaping boundary. Hold overflow
is the same lag disconnect as the live queue.

`read` and `seek_ts` each run floor + query in one SQLite transaction so a
peer trim cannot turn a prefix into a silent empty page.

Corrupt envelope JSON is `TrailError::Corrupt`, not an empty-id skip.
Negative stored head/floor is corrupt. `seq > i64::MAX` is
`SeqOutOfRange` on write; reads from above that ceiling return empty.

## Limits

- Partition 0 only.
- `append([])` → `EmptyBatch`.
- `read` max `1..=4096`.
- Live queue 256; slow consumer is disconnected.
- Catch-up live-hold 256; overflow is lag disconnect.
- SQLite seq ceiling `i64::MAX`.
- Stream catch-up is tiers 1–2. Meaning/judge on live events during
  catch-up still run on the follower path. Stream+expect absence timers
  are not armed for catch-up-owned seqs.

## Protocol still local-shaped (ObjectLog later)

- Wire seq is one `u64`.
- Live tails still `try_send` at ingest; history is a log page, not Comb follow.
- Ingest still holds `Mutex<State>` across a local SQLite append.
- `deliveryId` is still `{sub}:{event.id}:{n}`.

## Checks

```
CARGO_TARGET_DIR=/Users/trmd/Projects/honeybee/pheromone/repos/pheromone/target
cargo test -p pher --offline -- --test-threads=1
cargo clippy -p pher --offline --tests -- -W clippy::all
rustfmt crates/pher/src/{traillog,daemon,db,http}.rs
```

No shared daemon. No `~/.pheromone`. No Comb edits. Not pushed.
