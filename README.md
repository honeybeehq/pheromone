# Pheromone

**A distributed event bus for agent ecosystems.** Emit signals from anything readable — agent harnesses, container runtimes, CI, log drains, crash trackers, metrics providers — and subscribe with one primitive:

```
pher when 'on hive.seal where payload.status == "blocked"
           meaning "agent stuck on auth or credentials" > 0.8
           then buz CL.6308 --tier next-tool'
```

"Listen for when X happens," where X can be an exact match, a boolean expression, a semantic
similarity match, or an LLM-evaluated predicate — compiled into a single cost cascade, cheap
tiers gating expensive ones.

Way, way simpler than Kafka. Specialized for agent production and consumption. Deeply
integrated with the Honeybee ecosystem (Honeybee, Pollinate, Apiary, Kit), pluggable into
everything else.

## Status

**Prototype (roadmap slices 1–5 substantially complete).** All four tiers of the cost
cascade are live: `on` → `where` → `meaning` → `judge`, end to end, with the local bus,
all sinks, delivery shaping, and the hive ledger tap.

What works today:

- **The full subscription language** — parser, canonical string + JSON forms with
  round-trip stability (`pher parse`, `pher fmt`), the `where` CEL-subset evaluator,
  subject trie with `*`/`**`, match-explanation blocks.
- **Offline authoring loop** — `pher test '<sub>' --against events.jsonl` dry-runs a
  subscription against a bag of events with per-tier verdicts (see `testdata/`).
- **A local bus** (`pher daemon run`) — `emit`, `when`, `ls`, `rm`, `tail` (cursor
  resume), `why <delivery-id>`, `why-not <sub-id> <event-id>`. JSONL event log +
  crash-safe state under `~/.pheromone/`; kill -9 loses nothing.
- **Sinks — all of them:** `cmd`, `emit` (hop-capped cycle guard, correlation preserved),
  `buz`, `http` (validated POST/PUT/... off-thread, failures come back as
  `pher.delivery.failed` bus events), `hermes`, `pol`, `hive` (CLI handoff).
- **Delivery shaping:** `every` (leading edge + trailing collapsed event per window) and
  `batch` (one delivery per window, capped queue with drop accounting), persisted so
  kill -9 keeps queued events.
- **Tier 4 (`judge`) is live:** yes/no verdicts from a cheap LLM on tier-3 survivors.
  Pluggable provider via `PHER_JUDGE_MODEL` — default `claude-haiku-4-5` (Anthropic
  Messages API with prompt caching on the stable question prefix + structured outputs),
  or any OpenAI model such as `gpt-5.6-luna` (chat completions). Async worker (verdicts
  deliver on completion, never blocking ingest), mandatory budgets that fail closed and
  emit `pher.subscription.budget_exhausted` exactly once per window, content-fingerprint
  verdict cache (replay and repeats never re-roll; `cached: true` in the match block),
  `sample` support, and redaction before anything leaves the process. Registration
  requires a working provider config (`ANTHROPIC_API_KEY` / `OPENAI_API_KEY`).
- **Tier 3 (`meaning`) is live:** local ONNX embeddings (bge-small, 384-dim, downloaded
  once to `~/.pheromone/models`, offline after), text projection + secret redaction,
  descriptor matching, and `novel` anomaly detection over a windowed vector log.
  Scores/thresholds/model id ride in every match block and `why-not`. Measured quality
  and threshold guidance: [docs/TIER3_QUALITY.md](docs/TIER3_QUALITY.md).
- **Lifecycle:** `for <ttl>` evaporation, `limit <n>` one/n-shot, `since <lookback>`
  replay, `expect ... within ... else` absence timers with `$origin` join — all working.
  Retention GC (default 7d, `PHER_RETENTION`) from day one.
- **Cross-node (hub-and-leaf over the tailnet):** `pher node add studio --url
  http://studio:4870 --token …` registers remote nodes; the global `--node`
  flag runs any command against them (`pher --node studio when …` — full
  protocol over `POST /rpc`). Filter-at-source is language composition: register
  the subscription ON the source node with `then http POST http://hub:4870/deliver`
  (source daemon authenticates via `PHER_HTTP_SINK_TOKEN`) and only matches
  cross the wire — origin envelope preserved, hop-capped, admission-deduped by
  deliveryId (at-least-once shipping, effectively-once ingestion). Leaf machines
  run taps with no local daemon: `pher --node hub tap hive`.
- **HTTP ingress** (`PHER_HTTP=127.0.0.1:4870`, or a tailnet address +
  `PHER_HTTP_TOKEN` — public binds without a token are refused at startup):
  `POST /webhook/<name>` (generic webhook tap, HMAC-SHA256 verified via
  `PHER_WEBHOOK_SECRET[_<NAME>]`, events land as `webhook.<name>`),
  `POST /emit` (remote emit from other nodes), `POST /metric` (datapoint
  intake), `GET /healthz`.
- **Metric condition engine** (`pher condition add p95_high --metric
  p95_latency --gt 800 --for 5m --label env=prod`): conditions evaluate at
  the tap edge and only transitions become events (`metric.condition.entered`
  / `.cleared`) — the raw firehose never hits the matcher or the log.
- **Hive ledger tap** (`pher tap hive`) — the flagship: follows
  `hive events --follow --json` and puts every ledger event on the bus as
  `hive.<type>`, correlation mapped from session/bee fields. The ~160-type
  stream with zero programmatic subscribers has its subscriber.
- **Code-based subscribers** — `then stream` + the `@pheromone/client` Node SDK
  (`sdk/node`): `pher.on('on ci.* where …', handler)` registers a
  connection-scoped subscription; the daemon evaluates the full cascade and
  pushes deliveries down the connection. The subscription is removed the moment
  the listener disconnects — the enforced form of `while <client> alive` (and
  `pher when '… then stream'` is refused: a stored stream sub with no listener
  would be a lie). Same surface for humans: `pher listen 'on demo.*'`. Works
  **across the mesh**: `POST /listen` streams chunked NDJSON (heartbeats for
  bounded disconnect detection), so `pher --node hub listen '…'` and
  `PherClient.remote(url, {token}).on('…', handler)` subscribe to a remote
  hub with no local daemon. The SDK also covers
  `emit`/`when`/`ls`/`rm`/`why`/`whyNot`/`status` locally and via `/rpc`.
- **Mesh robustness** — the two halves of the delivery guarantee:
  *everything reaches the hub* (the `http` sink runs through a persistent
  outbox — immediate attempt, then backoff retries up to 15m apart until the
  receiver accepts or the entry outlives retention; `pher.delivery.retrying`
  fires once on the transition, `pher.delivery.failed` only on giving up;
  the cross-node dedup window is persisted, so at-least-once shipping stays
  effectively-once across daemon restarts) and *everything at the hub reaches
  each consumer* (every delivery carries the log `seq`; `pher listen --after
  N` resumes exactly, `--cursor <name>` stores the position hub-side and
  commits as deliveries are processed — reconnect replays precisely the gap,
  no dupes, bounded by retention with the expired remainder reported in the
  ack). `pher cursor ls/rm` inspects positions. The hive tap reconnects with
  backoff and resumes from its last-forwarded ledger timestamp, deduping the
  overlap. Verified with an outage drill: hub down → forward queued →
  hub up → delayed arrival, exactly once, dedup surviving kill -9.
- **Composable buses (bridges + grants).** A bus is just a pherd; meshes are
  built from two primitives, both speaking the subscription language.
  *Bridges* pull: `[[bridge]] from/sub` (or `pher bridge add`) holds a durable
  filtered listen against an upstream bus and re-ingests deliveries locally —
  envelope identity preserved (same event id, hops incremented), admission
  deduped by event id, position cursor-resumed across outages, worker
  supervised by the daemon with backoff. Derived buses are just buses whose
  inputs are bridges. *Grants* bound tokens: `[[grant]] allow/emit` gives a
  named bearer token a consume filter and a publish filter — enforcement IS
  the matcher (tiers 1–2 only; deterministic authorization), applied at the
  listen stream and at emit admission. Grant tokens can emit (filtered),
  listen (filtered), and commit cursors; operating the bus requires the admin
  token. `pher grant ls` shows token fingerprints, never tokens.
  `GET /.well-known/pheromone` makes a bus discoverable. Honesty note:
  grants are boundary enforcement, not cryptography — delivered events
  belong to their recipient.
- **Declarative config** — `pheromone.toml` + `pher apply [--prune] [--dry-run]`
  (see `pheromone.example.toml`): named subscriptions, metric conditions, and
  the node registry, reconciled idempotently against the local daemon or a
  remote hub (`pher --node metal1 apply`). Named things are file-owned;
  ad-hoc `pher when` registrations are never touched. Node tokens via
  `token-env`, never in the file.
- **`@pheromone/core` Node addon** (napi-rs, `crates/pher-node`) — the same
  matcher in-process for TS consumers: `parse`/`fmt`/`canon`/`validate`/
  `evaluate`/`whyNot` plus a standing `Matcher` class. Build with
  `npm run build`, smoke-test with `node test.mjs`.
- **`@pheromone/wasm`** (`crates/pher-wasm`) — tiers 1–2 compiled to WebAssembly:
  `parse`/`canon`/`validate`/`evaluate`/`whyNot` plus `survives()` for
  filter-at-source (edge middleware, log shippers, browser playground — only
  survivors cross the wire). Build with `./build.sh`, test with `node test.mjs`.
- **Browser playground** (`playground/index.html`) — the language in the browser
  over the WASM matcher: live validate-as-you-type, canonical form, and match/
  reject/survivor verdicts against an event, with preset examples. Serve the
  repo root (`python3 -m http.server`) and open `/playground/`; requires the
  web bundle (`cd crates/pher-wasm && ./build.sh web`).
- **Benchmarks:** `cargo run --release -p pher-core --example bench` — with 10k
  standing subscriptions, ~49k events/sec through tiers 1–2 in a deliberately
  pathological workload (33% `ns.**` catch-alls ⇒ ~470 candidates/event) and
  ~290k events/sec in a realistic one (2% catch-alls). Roadmap target met. Key
  mechanics: trie candidates skip tier-1 rechecks, parse-time path binding, and
  a per-event path cache so each distinct `where` path resolves once per event.

- **Install & run as a service:** release artifacts + a `curl | sh` installer are
  wired via cargo-dist (`.github/workflows/release.yml`; a tag push publishes
  once the repo is public). `pher init` bootstraps the state dir and reports
  environment gaps with the exact fix for each; `pher daemon install` runs pherd
  under launchd (macOS) / a systemd user unit (Linux) — starts at login,
  restarts on crash (kill -9 verified), logs to `~/.pheromone/log/pherd.log`,
  captures `PHEROMONE_HOME` + `PHER_*` env at install time. `pher daemon
  status` / `uninstall` complete the set.

- **Connectors** (`pher connect`) — SaaS vendors as event sources, where a
  connector is DATA, not code: a TOML manifest (auth header shape, poll
  endpoints, watermark cursor, subject mapping) executed by one generic
  engine. `pher connect add sentry --token env:SENTRY_TOKEN --param
  org=acme` and `sentry.issue.*` events flow; built-in catalog (sentry,
  github, stripe) plus drop-in manifests in `~/.pheromone/connectors/`.
  First poll baselines (live-from-now); watermark + id-window dedup persist
  across restarts and vendor outages (verified: outage catch-up emits
  exactly once). The `[exec]` escape hatch supervises any process emitting
  envelope JSONL — a plugin protocol, not a plugin API. Tokens resolve
  CLI-side from literal / `env:VAR` / `cmd:…` (secret managers plug in via
  cmd:, never as a dependency) and are stored 0600, listed as fingerprints.
- **The live console** (`pher ui`) — served by the daemon itself at
  `http://<PHER_HTTP>/ui`, zero build step: live event feed (via the new
  `POST /tail` NDJSON endpoint — the raw firehose, admin-only, records no
  deliveries), an emit form, a live subscription tester that registers a
  real connection-scoped sub (full cascade: meaning scores and judge
  verdicts run for real), why-not explanations, subscription management,
  and recipe presets per tier. Works against remote hubs too (token field).

Build: `cargo build --release` → `target/release/pher`. Test: `cargo test`.
Release artifacts: `dist build` (cargo-dist).

Quickstart:

```bash
pher init                                    # create state dir, see what's missing
PHER_HTTP=127.0.0.1:4870 pher daemon install # supervised daemon (or: pher daemon run)
pher ui                                      # live console in the browser
pher listen 'on hive.seal where payload.status == "blocked"'       # shell 1
pher emit hive.seal --payload '{"status": "blocked"}'              # shell 2
pher why-not <sub-id> <event-id>   # when something doesn't fire
```

Copy-paste scenarios for every tier: [docs/RECIPES.md](docs/RECIPES.md).

The design corpus lives in `docs/`:

| Doc | What |
|---|---|
| [docs/DIRECTION.md](docs/DIRECTION.md) | The founding brief and mission |
| [docs/LANDSCAPE.md](docs/LANDSCAPE.md) | Prior-art research: what exists, what doesn't, the wedge |
| [docs/ECOSYSTEM_AUDIT.md](docs/ECOSYSTEM_AUDIT.md) | Map of existing Honeybee/Pollinate/Apiary event surfaces — what to reuse, what to fill |
| [docs/LANGUAGE.md](docs/LANGUAGE.md) | The subscription language specification (the core of the product) |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Technical foundation: Rust core, TS edges, storage, taps, cross-node |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Implementation slices and open questions |

New agents working in this repo: read [AGENTS.md](AGENTS.md) first.

## The one-paragraph pitch

Every layer of this exists somewhere — boolean event filters (Inngest, EventBridge), agent
message meshes (Solace, NATS), semantic matching over streams (Flink + LLM functions) — but no
shipping product combines (a) a lightweight distributed bus, (b) tiered matching from exact →
boolean → semantic → LLM-eval, and (c) agent-native ergonomics. Pheromone occupies that
combination. See LANDSCAPE.md for the receipts.
