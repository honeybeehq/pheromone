# Architecture

## Component split — each layer in its natural language

```
┌─────────────────────────────────────────────────────────────────┐
│  TS edges (thin, I/O-bound, never hot path)                     │
│    ecosystem taps (hive ledger, pollinate, apiary internals)    │
│    @pheromone/client SDK · kit MCP module (pher_* tools)        │
└──────────────┬──────────────────────────────────────────────────┘
               │ local unix socket / tailnet HTTP+WS
┌──────────────▼──────────────────────────────────────────────────┐
│  pherd (Rust, single static binary, one per node)               │
│    push-tap runtime (webhooks, log drains, OTLP receiver)       │
│    ingest → envelope → matcher → log → delivery                 │
│    WS event plane (cursor resume) · judge client · leaf mode    │
└──────────────┬──────────────────────────────────────────────────┘
┌──────────────▼──────────────────────────────────────────────────┐
│  pher-core (Rust crate — the crown jewel)                       │
│    envelope + canonical JSON · subject trie · where evaluator   │
│    vector matcher + novelty · match-explanation record          │
│    cursor protocol · delivery dedup                             │
│  compiled 3 ways: native (pherd) · napi-rs addon · WASM         │
└─────────────────────────────────────────────────────────────────┘
```

**One matcher, one test corpus, every deployment target.** The napi-rs addon gives Apiary
and TS harnesses an in-process matcher (validate-as-you-type, match preview, `why-not` in
the UI, zero daemon round-trips). The WASM build is the "bring the subscription to the data"
wedge: tiers 1–2 run at the source — Vercel edge middleware, Cloudflare workers, log-shipper
plugins, inside a customer's own service — and only survivors cross the wire. It also powers
a browser playground for subscription authoring (regex101-for-pher), which is how a matching
language gets adopted.

## Why Rust (and not Go, and not TS)

**Not TS:** the original TS lean was premised on personal-fleet scale (single-digit
events/sec) where ecosystem-module reuse dominated. The scope now includes log drains,
PostHog/Sentry/BetterStack streams, and metrics at larger orgs — firehose-shaped ingest
where the hot path (parse → subject trie → thousands of boolean evals/event → selective
embedding) needs a systems language, and "single static binary, no runtime" is table stakes
for corp distribution. TS survives where it earns it: taps that import ecosystem TS modules,
the SDK, the MCP module — all I/O-bound edges.

**Rust over Go, three arguments:**

1. **"TS bindings" is a requirement Rust wins outright.** napi-rs produces first-class
   in-process Node addons. Go's answer is "run a daemon and call it" — a client, not a
   binding.
2. **The WASM story is a structural differentiator.** Same matcher in edge/browser/embedded
   contexts. Go's WASM output is bloated and clunky; for Rust it's first-class. None of the
   landscape competitors can structurally match filter-at-source.
3. **Tier 3 is core product and the Rust inference stack (ort / fastembed-rs / candle) is
   materially better** than Go's sparsely-maintained cgo ONNX bindings.

Go's strongest cards, and why they don't win here: **cel-go** (canonical CEL) — defused
because `where` is our own documented CEL-syntax *subset* with our own conformance corpus;
we need a correct evaluator for our spec, not bug-for-bug CEL parity. **Embedded
NATS/JetStream** — tempting for transport/leaf nodes, but JetStream doesn't give the
SQL-queryable index the matcher/replay/why-not path needs (SQLite still required), so it
mainly buys transport at the cost of the WASM/bindings wedge. Fallback position: if
velocity-to-v1 ever trumps differentiation, Go + embedded NATS is the coherent plan B.

## Decisions table

| Decision | Pick | Notes |
|---|---|---|
| Core + daemon + CLI | Rust | `pher-core` crate; `pherd`; `pher` (clap). Single static binary, cross-compiled |
| Bindings | napi-rs (`@pheromone/core`) + WASM | Built from day one so the grammar is pressure-tested by consumers |
| Envelope | CloudEvents-compatible JSON | `{id, ts, node, source, type, subject, correlation, payload, ttlClass}`; free interop with Knative/EventBridge producers |
| State/index store | SQLite (WAL), one DB per node | `~/.pheromone/pher.db`: subscriptions, offsets, deliveries, timers (`expect`), FTS5, vectors |
| Vectors | sqlite-vec, same DB | 384-dim; no second process. FTS5 gives a free lexical tier |
| Event bodies | `EventLog` trait; SQLite locally, S2 for hub/cloud | v1: SQLite `events` table. Growth path: S2 (s2.dev, S3-backed streams; `s2-lite` self-host) as the hub/cloud log and trail-network transport — infinite retention, seq/ts/tail reads, per-tenant basin. SQLite stays the index. See [S2.md](S2.md). The trait keeps "simpler than Kafka" true while leaving the ceiling open |
| Embeddings | Local ONNX, bge-small class (384-dim), ort/fastembed-rs | ~30–80MB quantized, CPU ms-scale. One embed per event, shared across subscriptions. Provider API (Voyage/OpenAI) optional config — must work offline on a laptop |
| Judge | claude-haiku-class, batched, prompt-cached | Stable question prefix → cached; pluggable provider interface, one great default; hard budgets, fail-closed |
| IDs | Honeybee-style short ids (`PH.4k2`) | House style |
| Request plane | HTTP/JSON on unix socket local; tailnet TCP remote | Kit-style identity: machine tokens → `tailscale whois` → local ambient |
| Event plane | WebSocket push, cursor resume | Comb protocol generalized: monotonic `sequence`, `after`/`nextAfter`/`hasMore` per consumer. Reconnect = resume from offset (HSR re-adoption mechanics) |
| Cross-node | Filter-at-source, ship matches only | Subscriptions registered anywhere, pushed to origin nodes; tiers 3–4 run where budget/CPU lives. Hub-and-leaf, not full mesh |
| Leaf mode | `pherd leaf --parent <node>` | Ephemeral containers: no local durability, subscriptions leased to parent; container dies → receptors evaporate |
| Delivery semantics | At-least-once + `deliveryId` dedup | Comb-transport pattern; Connected-store admission pattern for cross-node effects |
| Delivery shaping | Delegate to Pollinate DeliveryManager semantics | Never reimplement debounce/batch/throttle |

## Ingestion design

**Taps are plugins; two kinds.** *Push taps* (HTTP receivers) live in `pherd`: generic
webhook, Vercel log drain, Sentry, PostHog, BetterStack, plus the universal ones below.
*Ecosystem taps* are thin TS processes feeding the local socket, because they import TS
modules: hive ledger (`followLedgerEvents` — the stream with ~160 event types and zero
subscribers today), pollinate ledger, comb runs, harness hooks, Apiary internals via the
transcript normalizers.

**OTLP is the universal tap.** A first-class OTLP receiver (logs + metrics + traces) covers
effectively every observability vendor and OTel collector/agent. Add Prometheus
`remote_write` and a Vector/Fluent Bit sink and "a huge number of metrics providers" is
three endpoints, not a hundred taps.

**Metrics never enter as raw datapoints.** Nobody subscribes to "a datapoint arrived"; they
subscribe to "p95 > 800ms for 5m". Metric taps evaluate window/threshold conditions at the
tap and emit **condition-transition events** (`metric.condition.entered` / `.cleared`). The
matcher stays stateless per-event, the firehose stays out of the log, and a metrics alert is
just `on metric.condition.entered where payload.name == "p95_latency"` — composable with
`meaning`/`judge` like everything else. Statefulness lives at the edges (taps, the `expect`
timer table), never in the matcher.

**Redaction before anything leaves an event.** The Honeybee haystack redaction pass (API
keys, bearer tokens, `sk-ant-*`, `gh*_`, `AKIA*`, JWTs) is ported into `pher-core` and runs
before text projection (tier 3), judging (tier 4), and any external sink.

## The matcher cascade, internally

Per ingested event: (1) subject trie lookup → candidate subscription set; (2) `from`
filter; (3) `where` evaluation per candidate — this is the per-event hot loop, target
thousands of evals in microseconds; (4) if any surviving candidate has a `meaning` clause,
compute the event's text projection + embedding **once**, then per-candidate ANN/threshold
checks; (5) surviving `judge` candidates enqueue for batched verdicts (async — tier-4
matches deliver on verdict, not on ingest); (6) matched → match-explanation record →
delivery queue with `deliveryId`.

Soak target for v1 on one node: **50k events/sec through tiers 1–2** with 10k standing
subscriptions on developer hardware. Benchmarks in-repo from the first slice; Inngest's
"accidentally quadratic" postmortem is the cautionary tale for the candidate-set data
structures.

## Security

- Tailnet is the transport boundary (Apiary peer precedent); kit-style identity layering:
  machine bearer tokens → `tailscale whois` → local ambient.
- Public ingress (webhooks from Vercel/Sentry/etc. to a laptop-homed daemon) reuses the
  Pollinate satellite pattern: dumb public relay, HMAC-signed forwards.
- Subscriptions are principals-scoped: a leased receptor can only deliver to sinks its
  registrant could reach (buz accept policies already gate agent interruption; gateway
  policy patterns apply for the rest).
- Secrets via Hem refs in tap/sink config, never inline.

## Observability of the trail itself

The trail eats its own dog food: `pher.subscription.registered/evaporated/budget_exhausted`,
`pher.delivery.failed/quarantined`, `pher.tap.up/down/lagging`, `pher.node.online/offline`
are ordinary events on the trail, subscribable like anything else. `pher doctor` for the
portless-style one-shot diagnosis.
