# Tier 3 (`meaning`) — measured quality

Slice 4's quality harness (`cargo run --release -p pher-embed --example quality`) gates this
tier per ROADMAP. Model: `bge-small-en-v1.5-q` (384-dim, quantized ONNX, local). Projection
v1: `type + subject + redacted key: value payload prose`, 2KB cap.

## Findings (2026-08-09, 12-case labeled corpus)

**Coarse topical descriptors separate cleanly.** OOM-vs-TypeError, auth-blocked-vs-done,
pool-exhaustion-vs-latency, deploy-vs-pageview: positives score 0.69–0.77, negatives
0.53–0.62. Accuracy 8/8 at threshold 0.65.

**Fine distinctions do not separate.** "Infra flake, *not a code bug*" scores an actual
assertion failure *higher* (0.685) than a runner-network failure (0.672); bi-encoders cannot
do negation or judgment. This is by design the `judge` tier's job (slice 5) — compose:
`meaning` for cheap semantic routing, `judge` for the fine call on survivors.

**Novelty separates by an order of magnitude.** Near-duplicate events score ~0.02 novelty
(1 − nearest-neighbor cosine); genuinely different events ~0.26+ even with related noise in
the window. Verified live: repeated sensor readings suppressed, a smoke alarm fired.

## Threshold guidance (bge-small cosine ranges are compressed)

| Use | Suggested threshold |
|---|---|
| Coarse descriptor routing | `> 0.6` – `> 0.7` (0.65 sweet spot on the corpus) |
| The language default (no threshold given) | 0.75 — conservative; will under-match on this model. Set thresholds explicitly. |
| Novelty (`meaning novel`) | `> 0.15` – `> 0.3` (repeat suppression ~0.02, distinct ~0.25+) |

Every delivery and `pher why-not` carries the actual score, threshold, and model id, so
tuning is a one-command loop.

## Open items carried forward

- Model swap = reindex: vectors are stamped with the model id from day one; changing models
  requires dropping `vectors.jsonl` (enforcement of the mismatch is TODO).
- Candidate models to benchmark against the corpus: nomic-embed (matryoshka 256), newer
  small models (ROADMAP open question 2).
- Replay (`since`) does not evaluate the meaning tier yet; registration warns.
- The labeled corpus should grow with real hive/Sentry/CI events as taps run.
