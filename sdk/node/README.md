# @pheromone/client

Talk to a Pheromone trail from code: emit events, register subscriptions,
and receive deliveries as callbacks.

```js
import { PherClient } from "@pheromone/client";

const pher = await PherClient.connect(); // local pherd unix socket

// Receive deliveries in-process. The full cascade (subject trie, where,
// meaning, judge) is evaluated by the daemon; only matches reach you.
const listener = await pher.on(
  'on ci.* where payload.conclusion == "failure"',
  (delivery) => {
    console.log(delivery.event.subject, delivery.match.tiers);
  },
);

await pher.emit("ci.github.run.completed", { conclusion: "failure" });

listener.close(); // subscription is removed the moment you hang up
```

## The model

`on()` registers a **connection-scoped** subscription (`then stream` is
appended if your text has no then-clause). It lives exactly as long as your
connection: process exit, crash, or `close()` all remove it — the enforced
form of the `while <client> alive` lease. There is no polling; the daemon
pushes deliveries down the socket.

For subscriptions that should **outlive your process**, use `when()` with a
push sink — the daemon delivers whether or not you are running:

```js
await pher.when("on ci.* where payload.conclusion == \"failure\" then buz operator");
```

## Remote nodes

The same API works against a remote hub over HTTP — verbs via `/rpc`,
`on()` via `/listen` (chunked NDJSON with heartbeats). A machine on the mesh
needs no local daemon to subscribe:

```js
const hub = PherClient.remote("http://metal1:4870", { token: process.env.PHER_HTTP_TOKEN });
await hub.emit("deploy.finished", { env: "prod" });
await hub.when("on deploy.* then buz operator"); // durable, runs on the hub
const sub = await hub.on("on deploy.* where payload.env == \"prod\"", (d) => {
  console.log("prod deploy:", d.event.payload);
});
```

Remote disconnect detection is write-bounded: a vanished client is torn down
on the next delivery or heartbeat (≤ ~30s), not instantly like the local
socket. `tail` stays local-only (debugging surface).

## Robust consumption (cursors + reconnect)

For consumers that must survive flaky links, sleep, and restarts:

```js
const sub = await hub.on(
  'on ci.* where payload.conclusion == "failure"',
  handleDelivery,
  { cursor: "ci-watcher", reconnect: true },
);
```

- `cursor` names a hub-side position. On attach, the hub replays exactly the
  deliveries this cursor missed (`sub.ack.resumedFrom` / `.replayed` /
  `.gapExpired`), then goes live. With `autoCommit` (default), the position
  advances only after your handler finishes a delivery: when it returns, or
  when the promise it returns resolves. Crash and restart, and you get
  redelivery from the last commit: at-least-once, no gaps.
- Async handlers run concurrently. The cursor commits the highest seq whose
  handler and every earlier handler have finished, so a slow delivery holds
  the cursor until it completes.
- A handler that throws or rejects is not committed. The listener emits
  `'error'` with the error (its `delivery` property is the failed delivery),
  and the cursor stops advancing for the rest of that connection. With
  `cursor`, the next attach (restart or `reconnect` after a connection loss)
  resumes from the last commit, so the failed delivery comes back. Without
  `cursor`, resume state is the client-side `lastSeq`, which counts a failed
  delivery as seen. Without an `'error'` listener, Node treats the failure
  as uncaught.
- The `'delivery'` event fires when a delivery arrives, right after the
  handler is called; it does not wait for an async handler.
- `reconnect` re-attaches with backoff on connection loss and resumes from
  the cursor (or the last seen `seq`); the listener emits `'reconnect'`.
- Catch-up replay is bounded by the hub's retention (default 7d). For
  `meaning`/`judge` subscriptions, catch-up runs the missed events through
  tiers 3–4. Judge verdicts (including sampled-out decisions) and the judge
  budget belong to the cursor (if any) plus the canonical subscription
  (listeners without a cursor on the same subscription share one budget), so a reattach reuses recorded verdicts instead of
  re-asking, and reconnecting does not refill the budget. Judge deliveries
  for one subscription always arrive in `seq` order: a cached verdict waits
  behind an earlier one still being judged.
- As catch-up evaluates events it sends checkpoints, and the cursor commits
  up to them, so events judged "no" are not evaluated again on the next
  attach. Catch-up evaluates at most 1000 tier-3/4 candidates per attach.
  When it has to stop early (judge budget exhausted, that limit, judge call
  failed, embedder or judge provider unavailable, or `meaning novel`
  history for the event incomplete), the hub sends a gap
  `{ subscription, from, through, commitThrough, reason }` and the listener
  emits `'gap'`.
  Pass `onGap` to handle it; the cursor moves past the range only after
  `onGap` finishes (up to `commitThrough`, which stays below events still
  waiting in a debounce/batch window). Without `onGap`, the gap is an `'error'` (with a `gap`
  property), or a process warning if nothing listens for `'error'`, and the
  cursor commits up to the gap but never past it, so the next attach retries
  the range.

Every delivery carries `seq` (the hub log position) if you'd rather track
resume state yourself (`on(sub, h, { after: lastSeq })`).

## API

- `PherClient.connect({home?})` / `PherClient.remote(url, {token?})`
- `emit(subject, payload?, {type?, source?, correlation?})`
- `on(subscription, handler?, {client?})` → `Listener` (`delivery`/`close`/`error` events, `close()`)
- `when(subscription)` → durable registration `{id, canonical, warnings}`
- `ls()` / `rm(id)` / `status()` / `why(deliveryId)` / `whyNot(subId, eventId)`
- `tail({after?, subject?}, handler?)` → raw event stream (debugging)

## Test

```
cargo build -p pher && npm test
```
