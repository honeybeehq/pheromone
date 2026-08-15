# Recipes

Copy-paste scenarios for every tier and mechanism. Each recipe is
self-contained; all of them assume a running daemon:

```bash
PHER_HTTP=127.0.0.1:4870 pher daemon run     # foreground, or: pher daemon install
pher ui                                       # the live console in your browser
```

The console (`pher ui`) can drive most of these interactively — its preset
buttons mirror the recipes here. The terminal forms below are the same
mechanics, scriptable.

## 1. Hello, cascade (tiers 1–2)

```bash
pher listen 'on demo.* where payload.n > 1'          # shell 1: live matches
pher emit demo.hello --payload '{"n": 5}'            # shell 2: matches
pher emit demo.hello --payload '{"n": 0}'            # filtered by where
pher emit other.thing --payload '{"n": 9}'           # filtered by subject
```

Expect exactly one delivery in shell 1. `Ctrl-C` shell 1 and the
subscription evaporates (`pher ls` shows nothing) — that's the lease.

## 2. Why didn't it fire?

```bash
pher when 'on ci.* where payload.conclusion == "failure" then cmd true' --name ci-demo
pher emit ci.run --payload '{"conclusion": "success"}'
pher ls            # note the sub id
pher tail --after 0 | tail -3        # note the event id
pher why-not PH.<sub> PH.<event>     # → rejectedAt: "where", with the expression
```

Every delivery also has a receipt: `pher why <deliveryId>`.

## 3. Meaning tier (local embeddings, no API)

Requires the embedding model (~30MB, downloads once; unset `PHER_EMBED=off`).

```bash
pher listen 'on crash.** meaning "OOM or memory pressure" > 0.55'
pher emit crash.sentry.backend --payload '{"title": "OOMKilled: worker exceeded 2Gi RSS"}'   # matches (~0.62)
pher emit crash.sentry.backend --payload '{"title": "TypeError: undefined is not a function"}' # rejected
```

The match block carries the score. Threshold guidance: [TIER3_QUALITY.md](TIER3_QUALITY.md)
— 0.65 separates coarse categories; fine distinctions belong to the judge.

Novelty (anomaly) instead of similarity:

```bash
pher listen 'on app.log.error meaning novel > 0.4 over 1h'
pher emit app.log.error --payload '{"msg": "connection refused to db-1"}'   # first time: novel
pher emit app.log.error --payload '{"msg": "connection refused to db-1"}'   # repeat: suppressed
pher emit app.log.error --payload '{"msg": "disk quota exceeded on /var"}'  # different: novel
```

## 4. Judge tier (LLM verdicts, budgeted)

Requires `ANTHROPIC_API_KEY` or `OPENAI_API_KEY` in the daemon's env
(model via `PHER_JUDGE_MODEL`, default claude-haiku-4-5). Costs real money;
budgets are mandatory and fail closed.

```bash
pher listen 'on agent.step judge "Is this agent stuck in a loop, repeating the same failing action?" budget 20/day'
pher emit agent.step --payload '{"history": ["npm test → FAIL", "npm test → FAIL", "npm test → FAIL"]}'
# verdict true → delivery with rationale (arrives async, ~a second later)
pher emit agent.step --payload '{"history": ["read docs", "wrote fix", "npm test → PASS"]}'
# verdict false → no delivery; the verdict is cached by content either way
```

Re-emitting identical content never re-rolls the judge (`cached: true`).

## 5. Absence detection (expect/else)

```bash
pher listen 'on job.started expect job.finished where correlation == $origin.correlation within 30s else'
pher emit job.started --correlation batch-7
# wait 30s without emitting job.finished → the ORIGIN event delivers, meaning "no follow-up came"
pher emit job.started --correlation batch-8
pher emit job.finished --correlation batch-8    # timer disarmed, nothing fires
```

## 6. Metrics → conditions → events

Raw datapoints never hit the trail; only condition transitions do.

```bash
pher condition add p95_high --metric p95_latency --gt 800 --for 10s --label env=prod
pher listen 'on metric.condition.**'
for v in 900 950 970; do
  curl -s -X POST http://127.0.0.1:4870/metric -d '{"name":"p95_latency","value":'$v',"labels":{"env":"prod"}}'
  sleep 5
done   # → one metric.condition.entered after the 10s hold
curl -s -X POST http://127.0.0.1:4870/metric -d '{"name":"p95_latency","value":400,"labels":{"env":"prod"}}'
       # → one metric.condition.cleared
```

## 7. Delivery shaping

```bash
pher listen 'on noisy.* every 30s'      # debounce: leading + one trailing collapse per window
for i in $(seq 1 10); do pher emit noisy.tick --payload "{\"n\": $i}"; done
# → delivery for n=1 now; ONE delivery for n=10 when the window closes (collapsed: 9)
```

## 8. The hive ledger, live

```bash
pher tap hive --since 15m &
pher listen 'on hive.seal'              # every seal across your agents
pher listen 'on hive.** meaning novel > 0.5 over 7d'   # only surprising agent behavior
```

## 9. Webhooks in (HMAC-verified)

```bash
# daemon env: PHER_WEBHOOK_SECRET_GITHUB=s3cret
pher listen 'on webhook.github where payload.action == "opened"'
BODY='{"action": "opened", "number": 42}'
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac s3cret | cut -d' ' -f2)
curl -s -X POST http://127.0.0.1:4870/webhook/github -H "x-hub-signature-256: sha256=$SIG" -d "$BODY"
```

## 10. Cursors: never miss an event

```bash
pher listen 'on ci.**' --cursor my-watcher     # process some events, Ctrl-C
pher emit ci.while.away --payload '{}'          # emitted while nobody listens
pher listen 'on ci.**' --cursor my-watcher     # → replays exactly what you missed
pher cursor ls
```

## 11. Two trails on one machine (bridge + grant)

```bash
# Trail A ("company") on 4890 with a grant:
PHEROMONE_HOME=/tmp/trail-a PHER_HTTP=127.0.0.1:4890 PHER_HTTP_TOKEN=admintok pher daemon run &
cat > /tmp/a.toml <<'EOF'
[[grant]]
name = "reader"
token = "reader-token-0123456789"
allow = 'on public.**'
EOF
PHEROMONE_HOME=/tmp/trail-a pher apply /tmp/a.toml

# Trail B follows public.** from A using the grant:
PHEROMONE_HOME=/tmp/trail-b pher daemon run &
PHEROMONE_HOME=/tmp/trail-b pher follow http://127.0.0.1:4890 \
  --token reader-token-0123456789 --sub 'on public.**'

PHEROMONE_HOME=/tmp/trail-a pher emit public.announce --payload '{"v": 1}'
PHEROMONE_HOME=/tmp/trail-b pher tail --after 0 | grep public.announce   # same event id, hops: 1
PHEROMONE_HOME=/tmp/trail-a pher emit private.secret --payload '{}'      # never crosses
```

## 12. SDK in five lines

```js
import { PherClient } from "@pheromone/client";
const pher = await PherClient.connect();
await pher.on('on demo.* where payload.n > 1', (d) =>
  console.log(d.event.subject, d.match.tiers));
await pher.emit("demo.hello", { n: 5 });
```

## 13. Connect a SaaS vendor

```bash
pher connect catalog                 # what's available
pher connect add github --token env:GITHUB_TOKEN --param owner=acme --param repo=api
pher listen 'on github.pushevent'    # pushes, PRs, releases… as trail events
# your own vendor: drop a manifest in ~/.pheromone/connectors/ — ~20 lines
# of TOML (auth header, poll URL, interval, subject template, id, watermark)
```

First poll establishes the watermark and emits nothing (live-from-now).
Cursors persist: daemon restarts and vendor outages never re-emit.
