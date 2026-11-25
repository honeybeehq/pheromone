# The Pheromone subscription language

The subscription is the product. This document is the specification of its language — the
make-or-break surface. The grammar is small on purpose; every construct below earns its place
by being either free, deterministic, or explicitly budgeted.

## Design stance

1. **One line registers a subscription, from anywhere.** A bee mid-task, a shell, an MCP tool
   call, a TOML file, an SDK call. The string form is canonical; it parses losslessly into a
   structured JSON form (and back) for programmatic use.
2. **Cost is syntactically honest.** Each matching tier is its own clause, ordered cheap →
   expensive: `on` → `where` → `meaning` → `judge`. Clauses AND together. The compiler needs
   zero cost inference — the cascade *is* the clause order. There is no way to put an LLM
   call in front of an index lookup. (OR across tiers is intentionally unsupported in v1;
   register multiple subscriptions.)
3. **Tiers 1–2 are deterministic; tiers 3–4 are probabilistic — and the language admits it.**
   `meaning` takes a threshold; `judge` takes a mandatory budget. Replay uses recorded
   verdicts, never re-rolls.
4. **The matcher is stateless per-event.** The only stateful construct is `expect` (absence),
   which is one timer table. No sequences, no sliding windows, no CEP.

## Shape

```
when
  on <subject-pattern>[, <subject-pattern>...]        # tier 1 — subject index, REQUIRED
  [from <source-or-node-pattern>]                     # tier 1 — provenance filter
  [where <expr>]                                      # tier 2 — boolean over envelope+payload
  [meaning <semantic-clause>]                         # tier 3 — embedding similarity
  [judge "<yes/no question>" budget <n>/<period>]     # tier 4 — LLM verdict
  [expect <subject-pattern> [where <expr>] within <dur> else]   # absence (see below)
then <action>
[for <ttl> | while <client> alive]                    # lifetime (while: the host enforces it)
[every <window> | batch <window>]                     # delivery shaping
[since <lookback>]                                    # replay backlog first, then go live
[limit <n>]                                           # max deliveries, then retire
```

`on` is **mandatory**. Every subscription gets a free subject-index prefilter; that is what
keeps ten thousand standing subscriptions cheap. `on **` is legal, but you had to type it.

## Grammar sketch (EBNF-ish, normative for the parser)

```
subscription  := "when" clause+ "then" action option*
clause        := on | from | where | meaning | judge | expect
on            := "on" subject ("," subject)*
subject       := token ("." token)*            ; token = ident | "*" | "**" (** terminal only)
from          := "from" pattern
where         := "where" expr                  ; expr = CEL-syntax subset, below
meaning       := "meaning" (descriptor | "any" "of" "[" descriptor ("," descriptor)* "]"
                 | "novel" [cmp number] ["over" duration])
descriptor    := string [cmp number]           ; default threshold 0.75
judge         := "judge" string "budget" int "/" period ["sample" float]
expect        := "expect" subject ["where" expr] "within" duration "else"
action        := sink rest-of-line             ; sink ∈ {buz, hive, http, cmd, hermes, pol, emit, stream}
option        := "for" duration | "while" ident "alive" | "every" duration
                 | "batch" duration | "since" duration | "limit" int
```

## The tiers

### Tier 1 — `on` (subjects) and `from` (provenance)

Hierarchical dot-separated subjects, NATS-style wildcards: `*` matches one token, `**`
matches the remainder (terminal position only). Matched against a subject trie at ingest;
effectively free.

Seed vocabulary: Honeybee's ~160 ledger types plus tap namespaces —
`hive.seal`, `hive.bee.*`, `hive.flight.**`, `pol.job.*`, `ci.github.run.completed`,
`otel.genai.span`, `metric.condition.entered`, `container.oom`, `crash.sentry.*`,
`vercel.log.*`, `posthog.event.*`, `apiary.session.*`.

`from` filters on source id / node name with the same pattern syntax.

### Tier 2 — `where` (boolean)

A **documented CEL-syntax subset**, evaluated against the canonical envelope. We own this
spec; conformance target is our test corpus, not bug-for-bug CEL parity. Syntax is kept
CEL-compatible so a later swap to a full CEL engine is non-breaking.

Bound identifiers: `type`, `subject`, `source`, `node`, `ts`, `sourceTs`, `correlation`,
`payload` (full dotted-path access, e.g. `payload.pr.labels`). `sourceTs` is absent when
the source gave no readable timestamp, so `has(sourceTs)` tells the two apart; see
[Two timestamps](#two-timestamps-ts-and-sourcets).

Supported (v1): `==` `!=` `<` `<=` `>` `>=` · `&&` `||` `!` · `in` (membership) ·
`matches` (RE2-class regex, no backtracking) · `has(payload.x.y)` (presence) ·
`size(x)` · string/number/bool/null literals, list literals · parentheses.

Explicitly deferred: arithmetic, macros (`exists`, `map`, `filter`), timestamps beyond `ts`
comparisons, custom functions. Add only with corpus tests.

This tier is where ~90% of real subscriptions end. It must be boringly excellent.

### Tier 3 — `meaning` (semantic)

The subscriber writes a prose description of what they're listening for. At registration the
description is embedded once. At match time the event's **text projection** is embedded once
*per event* (shared across all subscriptions) and compared by cosine similarity.

```
meaning "infra flake or transient runner failure, not a code bug" > 0.8
meaning any of ["stuck on credentials", "auth token expired", "permission denied loop"]
meaning novel > 0.75 over 7d
```

- Default threshold **0.75**; `any of` = max over descriptors.
- `novel` inverts the primitive: fires when the event's embedding is *far from every* event
  embedding in the window (default 7d) — anomaly detection from the same index. "Tell me
  when something happens that hasn't happened before" is the most-wanted fleet-operator
  feature and it costs one ANN query.
- **Text projection** (OPEN QUESTION, see ROADMAP): which fields feed the embedding, per
  event family. Naive stringify-everything is noisy for verbose payloads (transcripts).
  v1 rule: `type + subject + redacted-stringified payload`, byte-capped (~2KB), with
  per-tap overrides. Redaction pass runs **before** projection (reuse Honeybee's haystack
  redaction: API keys, bearer tokens, `sk-ant-*`, `gh*_`, `AKIA*`, JWTs).

### Tier 4 — `judge` (LLM verdict)

A yes/no question posed to a cheap model, which sees the envelope (post-redaction) and
returns `{verdict, rationale}` (one line). Only tier-3 survivors (or tier-2 survivors if no
`meaning` clause) reach it.

```
judge "Is this agent stuck in a loop, retrying the same failing action?" budget 200/day
judge "Is this crash user-facing?" budget 50/day sample 0.5
```

- `budget <n>/<period>` is **mandatory** — no unbounded LLM spend by omission. When the
  budget is exhausted, the subscription's judge tier fails closed (no match) and a
  `pher.subscription.budget_exhausted` event is emitted on the trail itself.
- Verdicts cached by `(subscription-id, event-fingerprint)`.
- Batched: multiple pending events per model call; the subscription question is a stable
  prompt prefix → prompt caching makes per-verdict cost tiny.
- The rationale rides along in the delivery's `match` block.

### `expect` — absence (the one stateful construct)

Agent fleets fail *silently*; the highest-value alarms are events that didn't happen.

```
when on hive.bee.spawned
expect hive.seal where correlation == $origin.correlation within 2h
else then buz operator --tier queue
```

- `$origin` binds the triggering event, so the expected event can join on its fields
  (typically `correlation`).
- Implementation is a single timer table: origin match arms a timer keyed by the join value;
  a matching expected event disarms it; expiry fires the action with the origin event as
  payload and `match.tier = "expect"`.
- This is the entire extent of statefulness in the matcher. Sequences, counts, and windows
  are permanently out of scope (see AGENTS.md principle 3).

## Lifecycle, delivery shaping, replay

- `for <ttl>` — evaporates after the duration (generalizes Pollinate's temporary hooks).
- `while <client> alive` — a lease: the receptor lives only as long as `<client>`. The
  language accepts it on any action and `pher-core` parses it into `lifetime: lease`, but
  the matcher ignores lifetimes: `evaluate` and `whyNot` give the same verdict with or
  without the lease. Enforcing it is the host's job:
  - **pherd** enforces it through a listener connection. Only `then stream` subscriptions
    carry it there (`pher listen`, SDK `.on()`); the connection closes, the receptor
    evaporates. `pher when` and `pher apply` refuse it on stored subscriptions: pherd has
    no liveness signal for the holder of a stored subscription, so it would deliver
    forever after the holder died. Bound a stored subscription with `for <ttl>` instead.
  - **Apiary**, which embeds `pher-core` without pherd, composes rules as
    `when … then stream while <bee id> alive` and retires them through Honeybee's bee
    lifecycle.
  - Leaf-node subscriptions (future, ephemeral containers) will be leased to the leaf's
    connection to its parent.

  `for` and `while` are mutually exclusive.
- `every <window>` / `batch <window>` — debounce/batch, **delegated to Pollinate's
  DeliveryManager semantics** (immediate/throttled/batched/debounced with persisted state).
- `since <lookback>` — replay: run the backlog through the matcher first, then continue
  live with no gap (comb-style cursor handoff). Replay never re-rolls tiers 3–4, and a new
  subscription has no recorded verdicts, so `since` is rejected on subscriptions with
  `meaning` or `judge`. `since ... reevaluate` (opt-in fresh evaluation under the judge
  budget) is designed but not implemented.
- Listener resume (`pher listen --cursor`/`--after`, SDK `cursor`/`after`/`reconnect`) is
  different: catch-up runs the missed events through `meaning` and `judge`, recording verdicts
  (and sampled-out decisions) and charging the judge budget like live matching. Verdicts and
  budget are keyed by the cursor (if any) plus the canonical subscription (so listeners without a
  cursor on the same subscription share one budget), so a
  reattach reuses recorded verdicts instead of re-rolling and does not refill the budget.
  Judge deliveries for a subscription leave in `seq` order. Catch-up sends
  `{"checkpoint": {subscription, seq}}` lines as it goes so cursors advance past evaluated
  non-matches, and evaluates at most 1000 tier-3/4 candidates per attach. `meaning novel` is
  scored against the events before it in the novelty window; catch-up records the vectors it
  embeds so later backlog events see them, and gaps if the window no longer reaches back far
  enough. If catch-up stops early (budget exhausted, that limit, judge failure, provider
  down, `novel` history incomplete), the stream carries an explicit
  `{"gap": {subscription, from, through, commitThrough, reason}}` line (`commitThrough` stays
  below events still waiting in a debounce/batch window); the SDK surfaces it as a `gap`
  (handled via `onGap`) or an error. Neither the SDK nor `pher listen --cursor` commits past
  an unhandled gap; `pher listen --accept-gaps` commits past it after reporting it on stderr.
- `limit <n>` — one-shot and n-shot subscriptions; `limit 1` + timeout is the primitive
  under `pher.wait()`.

### Two timestamps: `ts` and `sourceTs`

Every envelope has `ts`, the time the origin node admitted the event (RFC 3339 UTC,
millisecond precision; bridged and forwarded events keep their origin's `ts`). An event may
also carry `sourceTs`, the time its source says it happened: a connector reads it from the
record field its manifest names in `ts` (Sentry `lastSeen`, GitHub `created_at`, Stripe
`created`), and any emitter speaking the wire protocol (exec connector JSONL, socket `emit`, HTTP
`/emit`) may send it as `sourceTs`; the `pher emit` CLI does not set it. The origin node normalizes it to the same shape as `ts`, so
`where sourceTs < ts` compares correctly. Accepted: RFC 3339 with `Z` or a `±HH:MM` offset,
and Unix epoch seconds or milliseconds (as a number or numeric string), between 1970 and
year 9999. Anything else is dropped from the envelope, never the event: the event is
admitted without `sourceTs`, the daemon logs it, and an emit reply carries a `warnings`
entry. A connector logs the record id and the unreadable value.

`sourceTs` belongs to the event it arrived on. An event derived by `then emit` is a new
event without `sourceTs`; subscribe to the original subject when vendor time matters.

`ts` is authoritative. Trail order is append order (`seq`); cursors and catch-up resume by
`seq`; `since` lookbacks, replay seeks and retention (evaporation) cut by `ts`. `sourceTs`
is informational: it never reorders the trail and never decides what a lookback includes.
The reasons:

- Vendor clocks skew; one vendor's "now" is not ours, and two vendors disagree.
- Records arrive late or backdated: a connector catching up after an outage, a vendor
  that reports `lastSeen` for an issue first seen weeks ago, a Stripe event delivered on
  retry. Ordering by `sourceTs` would insert them into history that subscribers have
  already consumed, and `since 1h` would silently skip a record that arrived a minute ago
  but claims to be from yesterday.
- The trail is append-ordered by `seq`, and cursors and catch-up are seq positions. A
  second ordering by vendor time would break the gap-free handoff.

To filter on when something happened at the source, say so in `where`:
`on sentry.** where sourceTs >= "2026-11-25T00:00:00.000Z"`, or `!has(sourceTs)` for events
whose source gave no readable time.

## Two consumption modes, two verbs

**Standing subscription** (durable, multi-delivery): `pher when '...'`.

**One-shot wait** (ephemeral, single delivery, timeout-bearing) — a different verb because
it's a different lifecycle:

```ts
const ev = await pher.wait('on deploy.finished where payload.env == "prod"',
                           { timeout: "30m" });
```

Compiles to `when ... limit 1 for <timeout>` with delivery over the caller's connection.

## Actions

Sinks reuse Pollinate's action taxonomy exactly:

```
then buz <bee> [--tier interrupt|next-tool|queue|passive]   # attention-aware agent delivery
then hive spawn|send|flow ...                               # spawn/steer agents
then http POST <url> ...                                    # webhooks out
then cmd <shell>                                            # local command
then hermes <invoke>                                        # human notification channels
then pol fire <trigger>                                     # hand to Pollinate
then emit <subject>                                         # re-emit onto the trail (composition)
then stream                                                 # deliver to the connected listener (SDK / pher listen)
```

`then stream` is the code-based subscriber path: it can only be registered over a live
listener connection (`pher listen`, or an SDK client's `.on()`), deliveries are pushed
down that connection, and the subscription is removed the moment the listener
disconnects. It is the only place pherd accepts the lease lifetime (`while <client>
alive`), because connection liveness is how pherd enforces it. `pher when '… then stream'`
is refused: a stored stream subscription with no listener would be a lie.

`then emit` is how subscriptions compose into pipelines; the delivery's `match` block is
attached to the re-emitted event's metadata, and `correlation` is preserved — threads stay
intact across hops. Cycle guard: a hop-count in event metadata, hard-capped (default 8).

## The delivery contract

Every delivery = envelope + **match explanation block**. This is the explainability
commitment and it is load-bearing for trusting tiers 3–4:

```json
{
  "event": { "id": "PH.01jdq3v8k2x9m4n7p0r5s6t8wz", "ts": "...", "node": "trmd-mbp", "source": "tap.github",
             "type": "ci.github.run.completed", "subject": "ci.github.run.completed",
             "correlation": "HE.a3f", "payload": { "...": "..." } },
  "match": {
    "subscription": "PH.4k2",
    "tiers": ["on", "where", "meaning"],
    "where": { "expr": "payload.conclusion == \"failure\"", "result": true },
    "meaning": { "descriptor": "infra flake...", "score": 0.86, "threshold": 0.8 },
    "judge": null,
    "deliveryId": "PH.4k2:PH.01jdq3v8k2x9m4n7p0r5s6t8wz:1"
  }
}
```

`deliveryId` is the idempotency key (comb-transport pattern: at-least-once delivery,
effectively-once effects).

## Introspection

- `pher why <delivery-id>` — the full match record for a firing.
- `pher why-not <sub-id> <event-id>` — replay one event through one subscription; report the
  first tier that rejected it and why (subject miss / expr false with bindings / score below
  threshold / judge no + rationale / budget exhausted). Silent subscriptions are the #1
  debugging pain in every trigger system; this makes them a one-command diagnosis.
- `pher test '<subscription-string>' --against <event-id|file.json|--last 100>` — dry-run
  authoring loop. Same matcher via WASM powers a browser playground later.

## Worked examples

```bash
# Bread and butter — tier 2 only, evaporates after a shift
pher when 'on hive.seal where payload.status == "blocked"
           then buz CL.6308 --tier next-tool' --for 8h

# Full cascade: cheap filter gates semantic gates judge
pher when 'on ci.github.run.completed
           where payload.conclusion == "failure" && payload.branch == "main"
           meaning "infra flake or transient runner failure, not a code bug" > 0.8
           then hive spawn --flow rerun-and-triage'

# Novelty monitor over the whole fleet, batched every 30m
pher when 'on hive.** meaning novel > 0.75 over 7d then buz operator' --every 30m

# LLM-judged stuck-agent detector, budgeted
pher when 'on hive.session.*
           judge "Is this agent stuck in a loop, repeating the same failing action?" budget 200/day
           then buz operator --tier queue'

# Absence: spawned but never sealed
pher when 'on hive.bee.spawned
           expect hive.seal where correlation == $origin.correlation within 2h
           else then buz operator'

# Retroactive investigation, then live
pher when 'on crash.sentry.* where payload.level == "fatal"
           then cmd ./triage.sh' --since 24h

# Metrics (condition transitions from the tap — never raw datapoints)
pher when 'on metric.condition.entered where payload.name == "p95_latency" && payload.env == "prod"
           then hermes notify-oncall'
```

## Canonical structured form

The string form parses into (and serializes from) canonical JSON — the API/SDK/storage
representation:

```json
{
  "on": ["ci.github.run.completed"],
  "where": "payload.conclusion == \"failure\" && payload.branch == \"main\"",
  "meaning": { "descriptors": ["infra flake or transient runner failure, not a code bug"],
               "threshold": 0.8 },
  "judge": null,
  "then": { "sink": "hive", "args": ["spawn", "--flow", "rerun-and-triage"] },
  "lifetime": { "kind": "durable" },
  "delivery": { "mode": "immediate" },
  "replay": null,
  "limit": null
}
```

`pher parse` converts string → JSON; `pher fmt` converts JSON → canonical string. Round-trip
stability is a test-corpus invariant.
