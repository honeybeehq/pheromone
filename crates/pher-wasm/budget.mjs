import assert from "node:assert/strict";
import { readFileSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";

const MODULE_BUDGET_BYTES = 1_500_000;
const RSS_BUDGET_BYTES = 8_000_000;
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

const rules = Array.from({ length: RULE_COUNT }, (_, i) => ruleFor(i));
assert.equal(new Set(rules).size, RULE_COUNT, "rules must be distinct");

const before = settledRss();

pher.initSync({ module: bytes });
const instantiated = settledRss();
const parsed = rules.map((rule) => pher.parse(rule));
const afterParse = settledRss();
let matched = 0;
for (const rule of rules) {
  if (pher.evaluate(rule, envelope).includes('"outcome":"matched"')) matched++;
}

const after = settledRss();
const growth = after - before;
assert.equal(new Set(parsed).size, RULE_COUNT, "parsed rules must be distinct");
assert.ok(matched > 0 && matched < RULE_COUNT, `expected a mix of matches, got ${matched}`);

const mb = (n) => `${(n / 1_000_000).toFixed(3)} MB`;
console.log(`module size: ${moduleBytes} bytes (${mb(moduleBytes)}), budget ${mb(MODULE_BUDGET_BYTES)}`);
console.log(
  `rss growth: ${growth} bytes (${mb(growth)}) from ${mb(before)} to ${mb(after)} ` +
    `for ${RULE_COUNT} rules parsed + evaluated (${matched} matched), budget ${mb(RSS_BUDGET_BYTES)}`,
);
console.log(
  `rss by stage: instantiate +${mb(instantiated - before)}, parse +${mb(afterParse - instantiated)}, ` +
    `evaluate +${mb(after - afterParse)}`,
);

const failures = [];
if (moduleBytes > MODULE_BUDGET_BYTES) failures.push(`module is ${mb(moduleBytes)}, over ${mb(MODULE_BUDGET_BYTES)}`);
if (growth > RSS_BUDGET_BYTES) failures.push(`rss grew ${mb(growth)}, over ${mb(RSS_BUDGET_BYTES)}`);
if (failures.length) {
  console.error(`@pheromone/wasm budget FAILED: ${failures.join("; ")}`);
  process.exit(1);
}
console.log("@pheromone/wasm budget: within size and memory budget");
