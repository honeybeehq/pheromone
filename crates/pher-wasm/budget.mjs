import assert from "node:assert/strict";
import { readFileSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";

const MODULE_BUDGET_BYTES = 1_500_000;
const TOTAL_RSS_BUDGET_BYTES = 24_000_000;
const MARGINAL_RSS_BUDGET_BYTES = 8_000_000;
const WARM_UP_RULES = 256;
const RULE_COUNT = 4096;

assert.equal(typeof globalThis.gc, "function", "run with node --expose-gc");

const here = (path) => fileURLToPath(new URL(path, import.meta.url));
const wasmPath = here("./pkg/pher_wasm_bg.wasm");
const moduleBytes = statSync(wasmPath).size;

const pher = await import(here("./pkg/pher_wasm.js"));
const bytes = readFileSync(wasmPath);

function settledRss() {
  for (let i = 0; i < 3; i++) globalThis.gc();
  return process.memoryUsage().rss;
}

function ruleFor(i) {
  const kinds = ["failed", "blocked", "stuck", "done", "spawn_failed", "timeout", "retry"];
  return (
    `when on fleet.svc${i}.event.*, fleet.shared.* ` +
    `where payload.kind == "${kinds[i % kinds.length]}" && payload.attempt >= ${i % 5} && payload.bee != "CL.${i}" ` +
    `then stream while apiary-${i.toString(16).padStart(4, "0")} alive`
  );
}

const envelope = JSON.stringify({
  id: "PH.01jdq3v8k2x9m4n7p0r5s6t8wz",
  ts: "2026-11-25T10:00:00Z",
  node: "n1",
  source: "hive",
  type: "fleet.shared.flag",
  subject: "fleet.shared.flag",
  payload: { kind: "failed", attempt: 3, bee: "CL.7" },
});

const warmUp = Array.from({ length: WARM_UP_RULES }, (_, i) => ruleFor(RULE_COUNT + i));
const rules = Array.from({ length: RULE_COUNT }, (_, i) => ruleFor(i));
assert.equal(new Set([...warmUp, ...rules]).size, WARM_UP_RULES + RULE_COUNT, "rules must be distinct");

function parseAndEvaluate(batch) {
  const parsed = batch.map((rule) => pher.parse(rule));
  let matched = 0;
  for (const rule of batch) {
    if (pher.evaluate(rule, envelope).includes('"outcome":"matched"')) matched++;
  }
  return { parsed, matched };
}

const glueLoaded = settledRss();
pher.initSync({ module: bytes });
const instantiated = settledRss();
const warm = parseAndEvaluate(warmUp);
const warmedUp = settledRss();
const { parsed, matched } = parseAndEvaluate(rules);
const done = settledRss();

assert.equal(new Set(parsed).size, RULE_COUNT, "parsed rules must be distinct");
assert.ok(matched > 0 && matched < RULE_COUNT, `expected a mix of matches, got ${matched}`);
assert.equal(warm.parsed.length, WARM_UP_RULES);

const total = done - glueLoaded;
const marginal = done - warmedUp;
const mb = (n) => `${(n / 1_000_000).toFixed(3)} MB`;

console.log(`module size: ${moduleBytes} bytes (${mb(moduleBytes)}), budget ${mb(MODULE_BUDGET_BYTES)}`);
console.log(
  `total rss growth: ${total} bytes (${mb(total)}) from glue loaded to ${WARM_UP_RULES} + ${RULE_COUNT} rules ` +
    `parsed and evaluated (${matched} of ${RULE_COUNT} matched), budget ${mb(TOTAL_RSS_BUDGET_BYTES)}`,
);
console.log(
  `marginal rss growth: ${marginal} bytes (${mb(marginal)}) for ${RULE_COUNT} rules after a ${WARM_UP_RULES}-rule warm-up, ` +
    `budget ${mb(MARGINAL_RSS_BUDGET_BYTES)}`,
);
console.log(
  `rss by stage: instantiate +${mb(instantiated - glueLoaded)}, warm-up +${mb(warmedUp - instantiated)}, ` +
    `${RULE_COUNT} rules +${mb(done - warmedUp)} (glue loaded at ${mb(glueLoaded)})`,
);

const failures = [];
if (moduleBytes > MODULE_BUDGET_BYTES) failures.push(`module is ${mb(moduleBytes)}, over ${mb(MODULE_BUDGET_BYTES)}`);
if (total > TOTAL_RSS_BUDGET_BYTES) failures.push(`total rss grew ${mb(total)}, over ${mb(TOTAL_RSS_BUDGET_BYTES)}`);
if (marginal > MARGINAL_RSS_BUDGET_BYTES) {
  failures.push(`marginal rss grew ${mb(marginal)}, over ${mb(MARGINAL_RSS_BUDGET_BYTES)}`);
}
if (failures.length) {
  console.error(`@pheromone/wasm budget FAILED: ${failures.join("; ")}`);
  process.exit(1);
}
console.log("@pheromone/wasm budget: within size, total and marginal memory budgets");
