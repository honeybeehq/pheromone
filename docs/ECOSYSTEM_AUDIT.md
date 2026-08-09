# Ecosystem audit: the existing event/trigger/messaging surface (audited 2026-08-08)

A map of what Honeybee, Pollinate, Apiary, Kit, and friends already have — what Pheromone
must reuse, and the gaps only Pheromone fills. File pointers are from the audit date; verify
before depending on them.

Repo layout: `/Users/trmd/Projects/trmd/<project>/repos/<repo>` — apiary, honeybee,
pollinate, kit, nectar (TS), mesh (Go). Hermes is a third-party installed Python agent
(`~/.hermes/`), not a first-party repo.

## 1. Pollinate — the trigger substrate

Charter: "owns *when* work fires. Does not reason, does not run agents in-process."

- **Sources (4):** `schedule` (cron/every/once + missed-fire policy), `poll`
  (command/http/file with cursor strategies), `webhook` (local `POST /hook/<path>` on
  `127.0.0.1:3978`, HMAC + transform), `manual`. Plus **temporary hooks** (TTL / one-shot,
  GC-swept) — the pattern Pheromone generalizes into subscription lifetimes.
- **Routers** (`src/router.ts`, plugin: `github-pr`): long-lived subject↔target bindings with
  per-subject locks, idle TTL, reconciling GC. The closest existing thing to stateful
  correlation — scoped to one trigger and one plugin namespace.
- **Delivery policies** (`src/delivery.ts`): immediate / throttled / batched / debounced
  (+ starvation cap, + per-trigger `maxConcurrent`), **with pending timers and queues
  persisted across daemon restarts** (`state/delivery-state.json`). Weeks of subtle work,
  already done. Pheromone delegates delivery shaping here or lifts this module — never
  reimplements it.
- **Actions:** command, http, emit, hermes, honeybee×7 (flow/loop/spawn/send/buz/kill/comb),
  sequence. Comb transport has typed retryable errors + `deliveryId`/`join-existing`
  idempotency — the at-least-once → effectively-once bridge, already solved once.
- **Matching — the key fact:** the entire matcher is **12 lines of top-level-key
  exact-equality** (`src/filter.ts`). No paths, no operators, no expressions, nothing
  semantic. Used in two places (trigger.filter, router.openWhen).
- **`emit` is a dead end:** `src/actions.ts` — an emit action appends
  `{event: "pollinate.emit", subject, payload}` to the ledger and stops. **Nothing
  subscribes.** No way for one trigger's output to activate another. This is the flagship
  bus-shaped hole.
- **Single node, single process.** The satellite (`src/satellite.ts`) is an inbound webhook
  relay (public host → home daemon, HMAC), not node-to-node event distribution.
- Store: plain files under `~/.pollinate` + `ledger.jsonl`. Job-retention GC exists because a
  **61,812-file backlog once OOM'd the daemon** — the origin of the GC-from-day-one rule.

## 2. Honeybee — the event backbone that has no subscribers

- **The ledger** (`~/.hive/ledger.jsonl`, `src/events.ts` + `src/store.ts appendLedger`):
  "the daemon's event substrate… a subscription surface without a daemon dependency for
  reads." Rotation-safe tail (`followLedgerEvents`, detects shrink + inode change), gap-free
  backlog→live handoff, glob filtering on `type`. **~160 namespaced event types** already:
  `session.*`, `bee.*`, `buz.*`, `seal`, `state.transition`, `flight.*`, `flow.run.*`,
  `loop.*`, `pool.*`, `comb.*`, `node.*`, `daemon.*`, `needs_input`, `question.asked`,
  `permission.asked`, …
- **Critical gap:** a repo-wide grep for the follow/collect APIs outside Honeybee returns
  **zero hits** in Apiary, Pollinate, and Kit. The stream is consumed only by humans at a
  terminal. Pheromone's hive tap is the missing subscriber, generalized.
- **buz** (`src/buz.ts`): strictly point-to-point (single recipient bee name — no topics, no
  broadcast, no wildcards), file-backed mailboxes, four attention tiers
  `interrupt → next-tool → queue → passive` with per-recipient accept policy and downgrade
  chain (passive is a hard floor — never silently dropped). Queue drained on
  `idle_with_output`, quarantine on repeated failure. **Pheromone delivers matches to bees
  *through* buz tiers** — attention-aware delivery no generic bus can offer. No wire
  protocol; mailboxes are machine-local.
- **Seals** (`src/seal.ts`): durable structured completion artifacts
  (status/summary/type/taskId/evidence), correlation via `taskId`. High-value event source.
- **Comb run events** (`src/comb/store.ts`): per-run `events.jsonl` with monotonic
  `sequence` and a **resumable cursor protocol — `after` / `nextAfter` / `hasMore`**. The
  best-engineered stream in the ecosystem; Pheromone generalizes exactly this contract from
  per-run to per-consumer offsets.
- **HSR remote relay** (`src/hsr/remoteTransport.ts`, `remoteEventMirror.ts`): ssh
  socket-forward + capped-backoff reconnect **with subscription re-adoption** and bounded
  inbound backpressure; mirrors remote per-bee events into local files. The ecosystem's only
  working cross-node event relay — single-purpose, per-bee. Proven mechanics to borrow for
  the event plane's resume semantics.
- **`hive search`** (`src/search/haystacks.ts`): lexical only, but **already includes secret
  redaction before indexing** (API keys, bearer tokens, `sk-ant-*`, `gh*_`, `AKIA*`, JWTs).
  Reuse this pass before any text leaves an event for embedding or judging.

## 3. Apiary — three internal event mechanisms, no pub/sub

- **Yjs CRDT docs** (layout / shared / ops) with append-only `.ylog`s — state replication
  that converges documents; not discrete addressed event delivery.
- **~60 bespoke Electron IPC push channels** (`apps/desktop/src/shared/api.ts`) with only
  `PushRevisionBroadcaster` (per-channel monotonic `rev`) as shared machinery. No topic
  registry, no filtering, no replay; dropped pushes recovered by full re-read. A future
  internal consumer of Pheromone-style discipline, and a cautionary tale.
- **Adapters** poll/watch upstream CLI stores (`packages/adapters/src/hive.ts`,
  `pollinate.ts`) with explicit "fs.watch is an invalidation hint, not a ledger" hedges.
  Also home of the **transcript normalizers** (per-harness claude/codex/grok/kimi/opencode →
  common `AgentEvent` stream) — directly reusable for harness taps.
- **Peer sync** (`apps/desktop/src/main/peer/`): tailnet discovery via
  `tailscale status --json`, WS on 8765, HMAC challenge/response pairing, "the tailnet is
  the transport-security boundary." Ready-made node-to-node wire patterns.
- **Connected command transport** (`apps/desktop/src/main/connected*`, SQLite via
  `node:sqlite`): durable admission-before-claim effect ledger, split checkpoint/effect DBs
  so a crash between them leaves no dispatchable effect, capability leases, signed proofs,
  canonical-JSON digests. **A durable, authorized, exactly-once effect core, already
  designed and built** — the pattern (not the code) for Pheromone's delivery dedup and
  cross-node admission.

## 4. Cross-node today: five unrelated mechanisms, no shared substrate

ssh-tmux nodes; HSR ssh-tunnel RPC + event mirror; Apiary peer WS (Yjs sync + ad-hoc
streams); Pollinate satellite (inbound webhook relay); kit MCP server (HTTP request/response,
port 7411, tailnet identity via `tailscale whois` — **no streaming/subscribe tool at all**,
the ledger is not network-exposed). Mesh (Go) is SSH fan-out convergence, explicitly not an
event substrate. **A bee on one machine cannot subscribe to an event produced on another.**

## 5. Hermes

Third-party Nous Research personal agent at `~/.hermes/`; the ecosystem's human-notification
delivery arm (telegram/discord/slack/signal/email/sms/ntfy/… — 26 channels). Integration is
one shell-out: `hermes <invoke>` with payload on stdin (Pollinate's `hermes` action). Keep as
a Pheromone sink for human-facing deliveries. Note: Hermes has its own routines/cron system
overlapping Pollinate schedules; unresolved, out of Pheromone's scope.

## 6. Vector/embedding infra

**None exists anywhere in the ecosystem.** No embedding call sites, no vector store, no ANN
index. Tier 3 starts from zero (model + store + ranking) but reuses the transcript
normalizers and the redaction pass for safe text extraction.

## 7. Reuse vs. rebuild — the scoping contract

**Reuse (layer over, never rebuild):**

| Primitive | Where | Role in Pheromone |
|---|---|---|
| Ledger follow/collect | honeybee `src/events.ts` | Producer-side tap, done |
| Comb cursor protocol | honeybee `src/comb/store.ts` | The consumer-offset contract |
| Delivery policies | pollinate `src/delivery.ts` | Delivery shaping (delegate or lift) |
| Router bindings + GC | pollinate `src/router.ts` | Correlation/binding patterns |
| Action taxonomy + templating | pollinate `src/actions.ts` | The sink set |
| Comb-transport idempotency | pollinate `src/comb-transport.ts` | At-least-once → effectively-once |
| Connected dispatch/effect ledger | apiary `connectedCommand*` | Exactly-once delivery pattern |
| Peer transport + discovery | apiary `main/peer/` | Node-to-node wire patterns |
| HSR relay mechanics | honeybee `src/hsr/` | Reconnect + re-adoption + backpressure |
| Kit MCP server | kit `src/server/` | Network/MCP exposure (`pher_*` tools) |
| Redaction + normalizers | honeybee haystacks, apiary adapters | Safe text for tiers 3–4 |
| GC/retention lessons | pollinate job GC, ledger rotation | Evaporation from day one |

**Gaps only Pheromone fills:** a subject namespace with wildcards and fan-out (buz is 1:1;
nothing has topics); a real matching language (exact-equality and type-globs are the current
ceiling); cross-node event delivery; durable consumer offsets (only combs have cursors, only
within one run); correlation across the hive/pollinate ledger boundary (no shared trace id
today); a subscriber surface for the hive ledger; network-exposed streaming in kit.

**The overlap warning, verbatim from the audit:** the risk is not duplicating one thing — it
is duplicating three things that are each ~70% built (Pollinate delivery+routing, Connected's
effect ledger, Honeybee's log+cursors). Pheromone's defensible scope is the connective
tissue implemented *over* those. Anything reimplementing debounce/batch/throttle, binding
GC, or an admission ledger is scope drift.
