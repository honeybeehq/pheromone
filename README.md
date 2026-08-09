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

**Prototype (roadmap slices 1–2, partial 3/5).** The language and the local bus work
end-to-end; tiers 3–4 (semantic/judge) parse but do not evaluate yet, and the daemon
refuses to register what it cannot honestly run.

What works today:

- **The full subscription language** — parser, canonical string + JSON forms with
  round-trip stability (`pher parse`, `pher fmt`), the `where` CEL-subset evaluator,
  subject trie with `*`/`**`, match-explanation blocks.
- **Offline authoring loop** — `pher test '<sub>' --against events.jsonl` dry-runs a
  subscription against a bag of events with per-tier verdicts (see `testdata/`).
- **A local bus** (`pher daemon run`) — `emit`, `when`, `ls`, `rm`, `tail` (cursor
  resume), `why <delivery-id>`, `why-not <sub-id> <event-id>`. JSONL event log +
  crash-safe state under `~/.pheromone/`; kill -9 loses nothing.
- **Sinks:** `cmd`, `emit` (re-emit with hop-capped cycle guard, correlation preserved),
  `buz` (best-effort `hive buz send`). Others are parsed but rejected at registration.
- **Lifecycle:** `for <ttl>` evaporation, `limit <n>` one/n-shot, `since <lookback>`
  replay, `expect ... within ... else` absence timers with `$origin` join — all working.
  Retention GC (default 7d, `PHER_RETENTION`) from day one.
- **Hive ledger tap** (`pher tap hive`) — the flagship: follows
  `hive events --follow --json` and puts every ledger event on the bus as
  `hive.<type>`, correlation mapped from session/bee fields. The ~160-type
  stream with zero programmatic subscribers has its subscriber.
- **`@pheromone/core` Node addon** (napi-rs, `crates/pher-node`) — the same
  matcher in-process for TS consumers: `parse`/`fmt`/`canon`/`validate`/
  `evaluate`/`whyNot` plus a standing `Matcher` class. Build with
  `npm run build`, smoke-test with `node test.mjs`.
- **Benchmarks:** `cargo run --release -p pher-core --example bench` — with 10k
  standing subscriptions, ~49k events/sec through tiers 1–2 in a deliberately
  pathological workload (33% `ns.**` catch-alls ⇒ ~470 candidates/event) and
  ~290k events/sec in a realistic one (2% catch-alls). Roadmap target met. Key
  mechanics: trie candidates skip tier-1 rechecks, parse-time path binding, and
  a per-event path cache so each distinct `where` path resolves once per event.

Build: `cargo build --release` → `target/release/pher`. Test: `cargo test`.

Quickstart (two shells):

```bash
pher daemon run                                                    # shell 1
pher when 'on hive.seal where payload.status == "blocked" then cmd echo stuck'  # shell 2
pher emit hive.seal --payload '{"status": "blocked"}'
pher why-not <sub-id> <event-id>   # when something doesn't fire
```

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
