# AGENTS.md

Orientation for agents working in this repo.

- Contact / owner: Tormod Haugland (@TormodHaugland, tormod.haugland@gmail.com)
- Project: **Pheromone** — distributed event bus for agent ecosystems. Part of the Honeybee
  umbrella (Honeybee, Pollinate, Apiary, Kit, Nectar, Mesh).
- Status: design phase complete, implementation starting. The design corpus in `docs/` is the
  source of truth. Read in this order: `DIRECTION.md` → `LANGUAGE.md` → `ARCHITECTURE.md` →
  `ROADMAP.md`. Consult `LANDSCAPE.md` and `ECOSYSTEM_AUDIT.md` when making scope or
  positioning decisions.

## Non-negotiable design principles

1. **Cost is syntactically honest.** The subscription language's clause order *is* the match
   cascade (`on` → `where` → `meaning` → `judge`). Never add a construct that hides an
   expensive tier behind a cheap-looking one.
2. **`on` is mandatory.** Every subscription has a free subject prefilter. This is what keeps
   ten thousand standing subscriptions cheap.
3. **The matcher is stateless per-event.** Statefulness lives at the edges: taps (metric
   condition windows) and the single `expect` timer table. No CEP, no sliding windows in the
   core. That's Flink's swamp; we don't enter it.
4. **Tiers 1–2 deterministic, tiers 3–4 probabilistic — and recorded.** Replay uses recorded
   verdicts, never re-rolls. Every delivery carries its `match` explanation block.
5. **Do not rebuild what the ecosystem already has 70% built.** Specifically: Pollinate's
   delivery policies (debounce/batch/throttle with persisted state) and router bindings;
   Apiary's Connected dispatch/effect ledger patterns; Honeybee's ledger + comb cursor
   protocol. Pheromone is connective tissue *over* these. If you find yourself implementing
   debounce or an admission ledger, stop and re-read ECOSYSTEM_AUDIT.md §7.
6. **GC and retention from day one.** Pollinate once OOM'd on a 61,812-file job backlog.
   Every log, index, and cache in Pheromone ships with an evaporation policy in the same PR
   that introduces it.
7. **Metrics enter as condition transitions, not datapoints.** Metric taps evaluate
   window/threshold conditions and emit `metric.condition.entered/.cleared` events. The raw
   firehose never hits the matcher or the log.

## Stack (decided — see ARCHITECTURE.md for rationale)

- **Core (`pher-core`): Rust.** Envelope, subject trie, `where` evaluator, vector matcher,
  cursors, dedup. Compiled three ways: native (daemon), napi-rs Node addon, WASM.
- **Daemon (`pherd`) + CLI (`pher`): Rust.** Single static binary.
- **TS at the edges:** ecosystem taps (import Honeybee/Pollinate/Apiary TS modules, feed the
  daemon over the local socket), `@pheromone/client` SDK, kit MCP module.
- Storage: SQLite (WAL) for state/index/FTS/vectors (sqlite-vec) + segmented append log for
  event bodies, behind a storage trait.
- Embeddings: local ONNX (bge-small class, 384-dim) via ort/fastembed-rs. Provider APIs
  optional, never required.
- Judge: claude-haiku-class model, batched, prompt-cached, hard budgets.

## Conventions

- Follow ecosystem house style: CLI-first, `--json` on every read command, daemonized via
  launchd/systemd like `pol daemon`, state under `~/.pheromone/`.
- IDs are Honeybee-style short ids (`PH.4k2`).
- Always typecheck/lint/build; high test coverage on logic. The matcher gets a golden test
  corpus and (eventually) fuzzing — it is the crown jewel.
- Use `trash` or POSIX equivalent for deleting.
- Sibling repos for reference (read-only from here):
  - Honeybee: `/Users/trmd/Projects/trmd/honeybee/repos/honeybee`
  - Pollinate: `/Users/trmd/Projects/trmd/pollinate/repos/pollinate`
  - Apiary: `/Users/trmd/Projects/trmd/apiary/repos/apiary`
  - Kit: `/Users/trmd/Projects/trmd/kit/repos/kit`

## Vocabulary (used lightly, in docs and UX copy — not in every identifier)

- **deposit / emit** — publish an event
- **receptor** — a standing subscription
- **trail** — a correlation chain of events (trigger → job → spawn → seal → …)
- **evaporation** — retention/TTL policy
