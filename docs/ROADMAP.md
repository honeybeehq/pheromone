# Roadmap

Slices are ordered so that each one pressure-tests the riskiest remaining assumption. Every
slice ships with tests, benchmarks where relevant, and evaporation/GC for anything it
persists (AGENTS.md principle 6).

## Slice 1 — `pher-core`: the language, executable

The crown jewel first, consumable from day one.

- Rust workspace: `pher-core` crate + `pher` CLI skeleton + `@pheromone/core` napi-rs addon.
- Envelope type + canonical JSON (CloudEvents-compatible).
- Grammar parser: subscription string → canonical JSON → canonical string (`pher parse`,
  `pher fmt`; round-trip stability is a corpus invariant).
- `where` evaluator (the CEL-syntax subset per LANGUAGE.md — paths, comparisons, `in`,
  `matches` RE2, `has`, `size`, boolean ops).
- Subject trie with `*`/`**` matching.
- Match-explanation record; `pher test '<sub>' --against <events.json>` dry-run.
- **Golden test corpus**: real events sampled from the hive ledger (~160 types) + synthetic
  edge cases; parser fuzzing.
- Benchmark harness: N subscriptions × M events through tiers 1–2 (target: 50k events/sec,
  10k subscriptions, tiers 1–2, one core-count machine).
- napi-rs addon published locally so Apiary can consume the matcher immediately — this
  pressure-tests both the grammar and the bindings story in one move.

**Exit criterion:** an agent (or Apiary) can parse, validate, explain, and dry-run any v1
subscription string against a bag of events, in-process, with no daemon.

## Slice 2 — `pherd`: the local trail

- Daemon: ingest endpoint (unix socket + localhost HTTP), SQLite (WAL) state store,
  segmented event log behind the storage trait, subject-trie hot path.
- `pher emit`, `pher when`, `pher ls`, `pher rm`, `pher tail` (WS event plane with comb-style
  cursor resume), `pher why` / `pher why-not`.
- Lifetimes: `for <ttl>`, `limit <n>`, lease heartbeats for `while <bee> alive`.
- Delivery: at-least-once with `deliveryId` dedup; sinks `cmd`, `http`, `emit` (with hop-cap
  cycle guard). Retention/evaporation policies + GC sweep.
- `pher wait` (one-shot mode) over the caller's connection.
- launchd/systemd install (`pher daemon install`), `pher doctor`.

**Exit criterion:** two shells on one machine: `pher when ... then cmd ...` in one,
`pher emit` in the other, delivery with full match block; kill -9 the daemon mid-stream,
restart, no loss, cursor resume.

## Slice 3 — first taps + ecosystem sinks

- **Hive ledger tap** (TS, `followLedgerEvents`) — the flagship: the stream with zero
  subscribers gets its subscriber. Correlation mapping from ledger fields.
- Generic webhook tap (HMAC, transform) + Pollinate ledger tap.
- Sinks: `buz` (tier-aware), `hive spawn/send/flow`, `hermes`, `pol fire`.
- Delivery shaping (`every`/`batch`) via Pollinate DeliveryManager semantics.
- Kit MCP module: `pher_when`, `pher_wait`, `pher_emit`, `pher_ls`, `pher_why_not`,
  `pher_tail` (the ecosystem's first network-exposed streaming tool).
- `@pheromone/client` TS SDK (`pher.when()`, `pher.wait()`, `pher.emit()`).

**Exit criterion:** `pher when 'on hive.seal where payload.status == "blocked" then buz
<bee> --tier next-tool' --while <bee> alive` works end-to-end on real hive traffic.

## Slice 4 — tier 3 (semantic + novelty)

- ONNX embedding runtime (ort/fastembed-rs), bge-small class, 384-dim; sqlite-vec index.
- Text projection v1 (+ per-tap overrides) with the ported redaction pass.
- `meaning` / `meaning any of` / `meaning novel` end-to-end; scores in match blocks;
  `since <lookback>` replay (recorded-verdict semantics).
- Quality harness: labeled corpus of (event, descriptor, should-match) triples; report
  precision/recall at default threshold. **This gates shipping tier 3** — match quality is
  the product here.

## Slice 5 — tier 4 (judge) + `expect`

- Batched judge client (claude-haiku-class), prompt caching, verdict cache, budgets with
  fail-closed + `budget_exhausted` trail event, `sample`.
- `expect ... within ... else` — the timer table, `$origin` binding.

## Slice 6 — cross-node

- Node registry + tailnet identity (kit patterns); subscription push to origin nodes;
  filter-at-source; match-only shipping with Connected-style admission dedup.
- `pherd leaf --parent <node>` for ephemeral containers (leased receptors, no local
  durability).
- Satellite relay for public ingress (Pollinate satellite pattern).

## Slice 7 — breadth (the corp story)

- OTLP receiver (logs/metrics/traces) + Prometheus `remote_write` + Vector/Fluent Bit sink.
- Metric condition-at-tap engine (`metric.condition.entered/.cleared`).
- Vercel log drain, Sentry, PostHog, BetterStack taps.
- WASM build of tiers 1–2 + edge filter example (Vercel middleware); browser playground.
- Pollinate integration both directions: `pol` `emit` action → pher subject;
  `source = pheromone` trigger type in Pollinate.

## Open questions (resolve before or during the slice that hits them)

1. **Text projection spec** (slice 4, the big one): which payload fields feed the embedding,
   per event family. Run the spike early: embed real hive-ledger + Sentry + CI events with
   2–3 candidate projections and eyeball nearest-neighbor quality before committing.
2. **Embedding model choice** (slice 4): bge-small vs nomic-embed (matryoshka 256-dim) vs
   newer small models; benchmark on the labeled corpus. Also: one model forever per index —
   model swap = reindex; version the index by model id from day one.
3. **`where` subset boundaries** (slice 1): is `matches` + `in` + `has` enough for the first
   50 real subscriptions? Collect misses from real usage before adding macros/arithmetic.
4. **Delivery-shaping ownership** (slice 3): lift Pollinate's DeliveryManager algorithm into
   Rust vs delegate to a Pollinate process. Leaning: reimplement the *algorithm* (it's small)
   but conform to its semantics + tests; revisit against AGENTS.md principle 5.
5. **Judge provider defaults** (slice 5): direct Anthropic API vs routing through kit for
   key management (Hem). Leaning: kit-managed key, direct API calls.
6. **Multi-tenancy model** (slice 7+): per-tenant subject-namespace prefixes vs separate
   DBs. Decide when the first external org shows up, not before — but keep tenant id in the
   envelope reserved-fields list now.
7. **Pheromone's own ledger vs hive ledger correlation**: adopt one `correlation` id scheme
   across hive + pol + pher so trails reconstruct across all three (today hive and pol
   ledgers share nothing). Coordinate with Honeybee before slice 3.

## Naming note

CLI is `pher`; ids are `PH.*`; state in `~/.pheromone/`. Vocabulary (deposit/receptor/
trail/evaporation) is docs-and-UX flavor, not identifier style — see AGENTS.md.
