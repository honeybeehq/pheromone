# Landscape: prior art and the gap (researched 2026-08-08)

**Bottom line: no shipping product combines (a) a lightweight distributed bus, (b) tiered
matching from exact → boolean → semantic → LLM-evaluated predicates, and (c) agent-native
ergonomics.** The boolean tier is thoroughly commoditized; the semantic/LLM tier exists only
inside heavyweight stream platforms and academic prototypes; the agent-native buses that
exist stop at topic/subject matching.

## 1. General event buses / brokers

- **NATS + JetStream** — lightweight edge-to-cloud pub/sub, hierarchical subject wildcards,
  durable streams; explicitly courting agents (Synadia shipped an Agents SDK on the new
  **NATS Agent Protocol**, May 2026: discovery, prompts, heartbeats over subjects). Matching
  is still subject wildcards + header filters. Strongest *substrate* comparison, not a
  matching competitor. The deployment-weight benchmark Pheromone should match.
- **Kafka / Redpanda / Pulsar** — partitioned logs; all content filtering lives in consumer
  code or a separate stream processor. The heavyweight baseline the proposal defines itself
  against. Redpanda's 2026 "Agentic Data Plane" is governance/identity for agents touching
  data, not subscription semantics.
- **MQTT (Mosquitto, EMQX, HiveMQ)** — topic wildcards; EMQX has a SQL-ish rule engine.
  Lightweight but no semantic tier, no agent ergonomics.
- **CloudEvents ecosystem** — Knative Triggers support **CESQL** boolean filter expressions
  (attribute-only, Kubernetes-bound). TriggerMesh (open-source EventBridge) is dead —
  repos archived through 2025. Apache EventMesh plateaued.

## 2. Event-driven developer platforms

- **Inngest** — the closest *ergonomic* prior art: event-triggered functions with **CEL
  filters**, and `step.waitForEvent()` pausing a run until a CEL-matching event arrives, at
  very large scale. Marketed at agent builders (AgentKit). Falls short: centralized
  cloud service, not a distributable bus; matching stops at boolean CEL; ingestion is
  "you send us events."
- **Hatchet** — near-twin: CEL event filters, durable event waits. Same gaps.
- **Trigger.dev** — cron/webhook triggers + waitpoints; no expressive predicate language.
- **Temporal** — signals address a *specific workflow ID*; no content-matched subscription.
- **Restate / Windmill** — key-based addressing / filtering-in-handler-code respectively.

## 3. Agent-specific infrastructure

- **Solace Agent Mesh** — open-source A2A-over-Solace-broker agent orchestration fabric.
  Closest thing marketed as an "event mesh for agents," but matching is topic-based
  delegation between registered agents; drags an enterprise broker along.
- **NATS Agent Protocol / Cotal** — agent presence, channels, addressing over NATS —
  transport and discovery, not predicates.
- **AgentBus (Kanevry/agentbus)** — literally "the open-source event bus for AI agents:
  webhooks in, agent actions out." Glob matching only, ~zero traction (2 stars). Evidence of
  demand, not competition.
- **Interop protocols** — MCP `resources/subscribe` (per-resource, no predicates); A2A
  per-task streams (point-to-point); AGNTCY/SLIM (transport + directory). Claude Code hooks
  are regex-matched lifecycle callbacks — a micro "when X" local to one harness.
- **Agent observability** — LangSmith **automation rules** (filter runs → webhook / LLM-judge
  → alert) is the only shipping LLM-evaluated-predicate → action loop, scoped to its own
  traces. Braintrust automations similar (SQL filters). Langfuse/Helicone/AgentOps are
  alerting-lite. None are buses.
- **Composio Triggers** — broad app-event ingestion (Slack/GitHub/etc.) delivered to agents;
  matching is per-integration event types only. Ingestion breadth without matching depth.

## 4. Semantic / content-based routing

- **Confluent Intelligence (Kafka + Flink)** — the only production stack where all four match
  tiers are *expressible*: `ML_PREDICT` (LLM calls from streaming SQL), real-time embeddings,
  `VECTOR_SEARCH` (Flink 2.2). But it *is* Kafka + Flink + a cloud contract, and a
  "subscription" is a deployed SQL job, not a one-call primitive.
- **semantic-router (Aurelio)** — embedding-similarity routing of utterances to routes; a
  request-routing library, not pub/sub. The reference implementation of the tier-3 primitive.
- **vLLM Semantic Router** — routes *requests to models*, not events to subscribers.
- pgvector-CDC matching exists only as blog-pattern plumbing. No named product.

## 5. Rules / CEP engines (boolean-tier prior art)

- **AWS EventBridge** — the UX benchmark for declarative content patterns (JSON pattern
  language, prefix/numeric/anything-but operators, huge fan-out). Cloud-locked, no semantic
  tier.
- **Flink CEP / Esper / Drools** — temporal pattern detection far beyond what Pheromone
  needs; correspondingly heavy. Explicit non-goal territory.
- **Zapier / Make / n8n filters** — proves the boolean-filter ergonomic ceiling is
  low-code-simple.

Because the boolean tier is solved many times over (CEL, CESQL, EventBridge patterns),
**Pheromone's differentiation cannot be tier 2**. It must be tiers 3–4 + deployment weight +
ingestion breadth.

## 6. Academic / emerging (2024–2026)

- **Neural Router: Semantic Content Matching for Agentic AI** (arXiv 2605.25701) — an LLM as
  the matching engine of a pub/sub broker for agents across edge-cloud, with
  embedding-cluster prefiltering and merged-evaluation cost amortization. Research prototype;
  strong validation that the idea is live and commercially unclaimed. Its amortization
  tricks apply directly to Pheromone's tier 4.
- **Governance-Aware Vector Subscriptions** (arXiv 2603.20833) — vector-similarity
  subscriptions to semantic regions of a knowledge base, composed with policy predicates.
- **Semantic Operators** (VLDB'25) — LLM predicates as optimizable query operators; the
  cascade playbook (cheap filter → embedding → LLM) transfers directly to Pheromone's
  matcher.
- Pre-LLM semantic pub/sub research (ontology-based; IBM patents) never escaped the lab
  because the matcher was the missing piece — which LLMs now supply.

## Closest competitors and where they fall short

1. **Inngest** (Hatchet as near-twin) — has the "listen for when X" primitive with CEL, at
   scale. Gaps: centralized orchestrator, not a bus; boolean ceiling; no native taps for
   harnesses/containers/observability.
2. **Solace Agent Mesh** (NATS AP / Cotal as open variants) — real distribution + agent
   ergonomics. Gaps: topic matching only; enterprise broker weight; non-agent ingestion is
   connector work.
3. **Confluent Intelligence** — all four tiers expressible. Gaps: maximal operational weight;
   subscriptions are SQL jobs; no agent-native delivery or sources.

## Sources

- NATS agent protocol: https://nats.io/blog/nats-native-protocol-for-ai-agents/
- Cotal: https://cotal.ai/ · https://github.com/Cotal-AI/Cotal
- Solace Agent Mesh: https://solace.com/products/agent-mesh/ · https://github.com/SolaceLabs/solace-agent-mesh
- AgentBus: https://github.com/Kanevry/agentbus
- Redpanda ADP: https://www.redpanda.com/blog/agentic-data-plane-adp
- Inngest: https://www.inngest.com/docs/features/inngest-functions/steps-workflows/wait-for-event · https://www.inngest.com/blog/accidentally-quadratic-evaluating-trillions-of-event-matches-in-real-time
- Hatchet: https://docs.hatchet.run/home/run-on-event · https://docs.hatchet.run/v1/durable-event-waits
- Knative CESQL triggers: https://knative.dev/docs/eventing/triggers/
- Composio triggers: https://docs.composio.dev/docs/triggers
- LangSmith rules: https://docs.langchain.com/langsmith/rules · online LLM-judge evaluators: https://docs.langchain.com/langsmith/online-evaluations-llm-as-judge
- Braintrust automations: https://www.braintrust.dev/docs/guides/automations
- AGNTCY / SLIM: https://agntcy.org/ · https://spec.slim.agntcy.org/draft-mpsb-agntcy-messaging.html
- Confluent Intelligence: https://www.confluent.io/product/confluent-intelligence/ · Flink model inference: https://docs.confluent.io/cloud/current/flink/reference/functions/model-inference-functions.html
- Flink 2.2 VECTOR_SEARCH: https://flink.apache.org/2025/12/04/apache-flink-2.2.0-advancing-real-time-data--ai-and-empowering-stream-processing-for-the-ai-era/
- semantic-router: https://www.aurelio.ai/semantic-router · vLLM semantic router: https://github.com/vllm-project/semantic-router
- Neural Router: https://arxiv.org/abs/2605.25701
- Governance-aware vector subscriptions: https://arxiv.org/abs/2603.20833
- Semantic Operators (VLDB): https://www.vldb.org/pvldb/vol18/p4171-patel.pdf
