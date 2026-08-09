# Direction

## The founding brief

> *Tormod Haugland, 2026-08-08 (lightly edited):*
>
> I want a new complementary tool for the Honeybee umbrella: **Pheromone** — a distributed
> event bus system across all our nodes, satellites, and ephemeral containers. It plugs deeply
> into all our current systems, but is also pluggable into far more general agent software:
> all harnesses, all container software, all cloud agent software. And beyond agents, it
> should be pluggable into ANY infra provider, software provider, analytics provider,
> monitoring system, crash alert system — really anything that's readable whatsoever.
>
> The purpose of the event bus is to make REALLY easy any sort of **"listen for when X
> happens"** — where X is any type of logical match: exact match, complex boolean match,
> semantic (vector) match, maybe even LLM evaluation.
>
> We are somewhat moving into lightweight-Kafka territory, but way, way simpler, and
> specialized for agent production and consumption.

Scope addendum (same session): this is not only a personal-fleet tool. It must ingest
Vercel, PostHog, Sentry, BetterStack, arbitrary logs, and a huge number of metrics sources
going forward, and be deployable at much larger organizations. That requirement drove the
compiled-core (Rust) decision — see ARCHITECTURE.md.

## The four founding questions, answered

**1. Does this already exist?** No. Every layer exists somewhere; no product combines a
lightweight distributed bus + tiered exact→boolean→semantic→LLM matching + agent-native
ergonomics. Two 2026 papers describe the matching engine almost verbatim with no product
behind them. Full analysis: LANDSCAPE.md.

**2. What makes it great, not just good?** The subscription as a one-call primitive; a
cost-honest tiered cascade; subscriptions with agent-shaped lifetimes (die with the bee that
registered them); semantic *novelty* detection; replay/time-travel subscription; explainable
matches (`why` / `why-not`); absence detection (`expect`); ingest-from-anything via taps and
a CloudEvents-compatible envelope. Full spec: LANGUAGE.md.

**3. Technical foundation?** Rust core compiled three ways (daemon, napi-rs Node addon,
WASM), TS taps/SDK at the edges, SQLite + segmented log storage, local ONNX embeddings,
budgeted LLM judge, filter-at-source cross-node fan-out over the tailnet. Full detail:
ARCHITECTURE.md.

**4. New system or Pollinate expansion?** New system — confirmed against the code. Pollinate
owns *when work fires* (activation → action; its matcher is 12 lines of exact-equality; its
`emit` action is a dead end; it is single-node by design). Pheromone owns the *event fabric*:
subjects, matching, offsets, fan-out, cross-node. Pollinate becomes both producer (`emit` →
Pheromone subjects) and consumer (a `pheromone` trigger source), gaining four-tier matching
for free without changing its identity. The strongest internal evidence a bus is the missing
piece: Honeybee's ledger stream has ~160 well-namespaced event types and **zero programmatic
subscribers** anywhere in the ecosystem. Full audit: ECOSYSTEM_AUDIT.md.

## Positioning in one sentence

Pheromone is the subscription layer the agent world doesn't have: NATS-class deployment
weight, EventBridge-class boolean filters, and the semantic/LLM tiers nobody ships — with
first-party taps for the systems agents actually live in.

## The three-legged wedge (each competitor owns at most one leg)

1. **The tiered matcher as one subscription API** with an automatic cost cascade
   (exact → boolean → embedding → LLM verdict, each tier gating the next).
2. **NATS-class deployment weight** — single static binary, leaf nodes for satellites and
   ephemeral containers — versus Kafka/Flink stacks or SaaS orchestrators.
3. **First-party readers for agent-ecosystem sources** (harness hooks, agent ledgers, OTel
   GenAI spans, container events, CI, crash trackers, log drains) treated as native inputs,
   not connector afterthoughts.

Plus one structural differentiator that falls out of the Rust/WASM decision: **bring the
subscription to the data** — tiers 1–2 of the matcher run *at the source* (edge middleware,
log-shipper plugin, inside a customer's own service), so only survivors cross the wire.
