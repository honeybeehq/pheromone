# @pheromone/client

Talk to the Pheromone event bus from code: emit events, register subscriptions,
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
  advances as your handler processes deliveries — crash and restart, and you
  get redelivery from the last commit: at-least-once, no gaps, no dupes.
- `reconnect` re-attaches with backoff on connection loss and resumes from
  the cursor (or the last seen `seq`); the listener emits `'reconnect'`.
- Catch-up replay is bounded by the hub's retention (default 7d) and
  evaluates tiers 1–2 only — the ack carries a warning for meaning/judge
  subscriptions.

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
