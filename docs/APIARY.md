# Pheromone in Apiary: what the engine owes

Apiary is making Pheromone its event bus. This note tells engine maintainers what that
design needs from this repo. It designs no fixes: engine fixes belong to the Apiary Project
"Pheromone engine fixes".

**Source.** Apiary repo (`github.com/honeybeehq/apiary`) `main`, commit `e58240756`:
`docs/design/event-bus/README.md` and `docs/design/event-bus/design.md` (§1 placement and
hosting, §9 gaps, §10 milestones, §12 dependencies). The human accepted the design on
2026-11-25; if it changes, this note follows it. Engine paths below are checked against
`origin/main` at `487691f`. The design itself cites engine `970d981`.

## 1. Pheromone's role in Apiary

Apiary hosts the **language**, not the daemon (design §1.1).

- `pher-core` is compiled to WASM (`crates/pher-wasm`) and inlined into the kernel's
  single-file bundle, which fleet updates ship to macOS and Linux machines (§1.2).
- An in-kernel plugin, `events`, does what `pherd` would do: log, subscriptions, lifetimes
  and delivery, as tables in the kernel's own SQLite store (§1.3, §1.5). No `pherd` runs and
  no Pheromone socket is opened.
- Only Home instantiates the module, on the first subscription, and drops it when the last
  one ends. Members never load it; they forward raw Honeybee events to Home (§1.2, §1.6).
- The napi addon (`crates/pher-node`) is rejected as the host: a native file per platform
  beside a one-file bundle, and a native fault kills the kernel. It stays the fallback if
  WASM misses its size budget (D3).
- Rejected too: porting the language to TypeScript, running `TrailLog`/Comb Log in process,
  and serving `pherd`'s wire protocol from the kernel (§1.5, §1.7).

**Consequences for the engine.**

- **`pher-core` and `pher-wasm` stay pure.** No file or network I/O, no timers and no
  threads in anything the kernel calls. The kernel has one deadline heap and bans other timers
  (§1.4, rule 3: "the WASM module has none"). The host passes in everything, including the
  module bytes (D2).
- **Lifetimes are the host's job.** `evaluate` and `whyNot` must keep ignoring lifetimes;
  Apiary enforces `for` and `while <bee> alive` from Honeybee's lifecycle (DL, §3.4).
- **Tiers 1–2 only, in process.** A rule with `meaning` or `judge` reports pending
  (`crates/pher-core/src/matcher.rs:92-94`); the host decides what happens next.
- **Stays pherd-only** (not used by Apiary): the event log and cursors, replay (`since`),
  sinks other than delivery to the host, connectors, the webhook endpoint, `every`/`batch`
  shaping (Pollinate owns shaping, polls and webhooks), the wire protocol, grants, leaf mode,
  `TrailLog`, `expect` timers, and tier 3 embeddings. Tier 3 (ONNX, about 185 MB resident,
  native threads) is refused in Apiary until the human decides Q9 (§1.2). Tier 4 runs later
  in a separate Apiary `judge` plugin, in TypeScript, with what D8 and D9 export (§1.2).

## 2. Engine dependencies (design §12)

Numbering is the design's. "Engine-fixes" says whether the Project "Pheromone engine fixes"
covers it. That Project's scope, as the Pheromone Champion recorded it on 2026-11-25, is seven
defects (event ids, `tap hive`, webhook auth, tier 3–4 replay, SDK cursor, connector
watermark, lease liveness) plus CI. None of the D-items are in that scope.

The design's M0 ("engine artifacts") is D2, D3 and DL, with D4 as a CI check (§10). M0 is
assigned to the engine-fixes Project in the design, but that Project's recorded scope does
not yet include it. D2 is done on `main` (`190536c`); D3 and DL remain.

| Id | Requirement | Blocks | Engine-fixes |
|---|---|---|---|
| D2 | A `pher-wasm` build whose glue instantiates from bytes handed in (no file reads, no `fetch`), so esbuild can inline it. | M1 | done in `190536c`: `--target web` (`crates/pher-wasm/build.sh:17`) with bytes-only `initSync` glue (`crates/pher-wasm/bytes-glue.mjs`), tested in CI |
| D3 | A size and memory budget measured in the engine's CI. Proposed: module ≤ 1.5 MB, resident increase ≤ 8 MB with 4,096 rules. Missing it sends Apiary to the napi fallback. | M1 | no |
| DL | `pher-core` keeps parsing `while <x> alive` on any action, including `then stream`, into `Lifetime::Lease`, and `evaluate`/`whyNot` keep ignoring lifetimes. `LANGUAGE.md` should say enforcement is the host's (`pherd`: a listener connection; Apiary: Honeybee's lifecycle) rather than only that `pher when` refuses it. | M1 (the contract must not regress) | related: the lease-liveness fix (`970d981`) changed only the daemon and docs |
| D4 | The language corpus exported as data (rule, envelope, expected result, expected `whyNot`) with versioned releases, so Apiary runs it in CI against its pin. Today it is Rust tests (`crates/pher-core/tests/corpus.rs`). | CI check from M1, not a blocker | no (its CI work is the engine's own suites) |
| D0 | A rule-only entry point (clauses without `then`) in `pher-core` and `pher-wasm`, with error positions. | nothing: Apiary composes `then stream` itself | no |
| D6 | `whyNot` tells a `where` that evaluated false from one that failed to evaluate. | M2 (the Why rail) | no |
| D1, D7 | A standing `Matcher` in `pher-wasm`; removal without a full trie rebuild; lookup without a linear scan. | before Apiary's rule cap rises above 4,096 | no |
| D8 | A pending match reports the tier-4 parameters (question, budget, sample). | M5 | no |
| D9 | The judge prompt contract and payload redaction exported from `pher-core` (`judge_prompt`, `redact`). | M4 (redaction), M5 | no |
| D10 | `expect` semantics for a host that owns its timers: deadline from the origin event's `ts`. | only if the human answers yes to Q11 | no |

Where each stands on `origin/main`:

- DL holds. Parsing: `crates/pher-core/src/subscription.rs:830-836` and `:867-872`;
  `Lifetime::Lease` at `:115`. Only the daemon refuses a stored lease
  (`crates/pher/src/daemon.rs:3170-3171`). `docs/LANGUAGE.md:158-162` and `:206-208` describe
  only the daemon's rule.
- D0: `then` is mandatory (`crates/pher-core/src/subscription.rs:141-146`, `:420-422`).
- D6: a false `where` and a failed one both return `Check::RejectWhere`
  (`crates/pher-core/src/matcher.rs:87-89`).
- D1, D7: the standing `Matcher` exists only in napi (`crates/pher-node/src/lib.rs:91-97`).
  `remove` rebuilds the trie and `get` scans linearly (`crates/pher-core/src/matcher.rs:248-264`).
- D8: `Check::Pending` carries no detail (`crates/pher-core/src/matcher.rs:92-94`).
- D9: the prompt (`crates/pher/src/judge.rs:78-88`) and the redacted, capped summary
  (`:89-103`) live in the daemon. `redact` lives in `pher-embed` (`crates/pher-embed/src/lib.rs:126`),
  the native ONNX crate.
- D10: the daemon arms `expect` from processing time (`crates/pher/src/daemon.rs:1805`).

**Known engine bugs (§12.2).** None blocks an Apiary milestone, because Apiary runs no
`pherd`. The design lists two that block only a later bridge from a standalone `pherd` into
Apiary; one of them is fixed on `main`. "Before `970d981`" marks fixes that were already in
the design's engine base.

| Bug | Blocks | Engine-fixes | On `origin/main` |
|---|---|---|---|
| 4-character event ids | was the later `pherd` bridge; now nothing | yes | fixed: ULID-style ids (`crates/pher-core/src/ids.rs:49-59`, `1638f92`, with `b0f5663` and `2124ecd` for legacy ids) |
| Delivery id not stable across replays | the later `pherd` bridge only | not in recorded scope | still a per-subscription counter (`crates/pher/src/daemon.rs:1502-1510`) |
| SDK early cursor commit; connector watermark and pagination; replay skipping tiers 3–4; tap incompatible with v2 | nothing in Apiary | yes | fixed: SDK `9f16bee` and replay `7076d44` (both before `970d981`); tap `8acf1db`, `3abebf5`; connector `6d2f855`, `464eb2f`. Replay is fixed by refusing `since` on `meaning`/`judge` rules (`crates/pher/src/daemon.rs:3180-3191`); replay still never evaluates tiers 3–4 |
| Seq reuse after full GC | nothing in Apiary | not in recorded scope | fixed: `51fcfbb`, via the comb-log merge `1ba0496` (before `970d981`) |
| Lossy control files; fire-and-forget process sinks; mutex poisoning; unbounded socket lines | nothing in Apiary | not in recorded scope | still open: `crates/pher/src/daemon.rs:370-380` reads control files with `unwrap_or_default`; `spawn_cli_sink` does not await exit (`:2120-2134`); 93 `.lock().unwrap()` in `daemon.rs`; `read_line` with no cap (`:2459`) |
| Unauthenticated `/webhook/<name>` | nothing | yes | fixed since `28d5740` |
| `feat/comb-log` not merged | nothing: Apiary's log is a kernel table | — | merged in `1ba0496`, already before `970d981` |

## 3. Standalone `pherd` and `pher` CLI users (design §1.7)

- **`pherd` stays a product of its own** for use outside Apiary. Apiary never starts,
  configures or reads it. Whether to retire the Mac Studio's idle `pherd` is the human's Q10.
- **`pher parse`, `pher fmt` and `pher test` keep validating Apiary rules.** They run
  `pher-core` offline, so a rule they accept is one the kernel accepts at the same version.
  Apiary's `events_list` reports the version the kernel pins.
- **`while <bee> alive` differs on purpose.** `pher when` refuses a lease on a stored
  subscription; Apiary accepts it because the kernel sees Honeybee's lifecycle.
- **The daemon commands do not reach Apiary.** `pher emit`, `when`, `listen`, `tail` and
  `why` speak `pherd`'s wire protocol, which the kernel does not serve. Inside Apiary,
  sessions use the `events_*` MCP tools and the operator uses the Events place.
- **`pher tap hive` is replaced** in Apiary by the kernel's own hived bridge (§2.7).
- **Later, on request:** a standalone `pherd` can feed Apiary through its `then http` sink
  posting to a grant hook (§5.3). The design says it waits for two bugs; only the unstable
  delivery id is still open.
