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

Non-streaming verbs work against any node's HTTP `/rpc`:

```js
const hub = PherClient.remote("http://hub:4870", { token: process.env.PHER_HTTP_TOKEN });
await hub.emit("deploy.finished", { env: "prod" });
await hub.when("on deploy.* then buz operator"); // durable, runs on the hub
```

Streaming (`on`/`tail`) over HTTP is not supported yet — run the SDK on the
node itself, or use a push sink pointed at your service.

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
