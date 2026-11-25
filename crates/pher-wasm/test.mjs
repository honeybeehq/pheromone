import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const here = (path) => fileURLToPath(new URL(path, import.meta.url));
const gluePath = here("./pkg/pher_wasm.js");
const glueSource = readFileSync(gluePath, "utf8");

const forbidden = [
  ["fs", /\bfs\b/],
  ["fetch", /\bfetch\b/],
  ["import.meta.url", /import\.meta/],
  ["require", /\brequire\b/],
  ["import statement", /^\s*import\b|\bimport\s*\(/m],
  ["process", /\bprocess\b/],
  [".wasm path", /pher_wasm_bg\.wasm/],
];
for (const [name, pattern] of forbidden) {
  assert.doesNotMatch(glueSource, pattern, `glue must not reference ${name}`);
}

const realFetch = globalThis.fetch;
globalThis.fetch = () => {
  throw new Error("glue attempted a network fetch");
};
const pher = await import(gluePath);
assert.deepEqual(Object.keys(pher).sort(), ["canon", "evaluate", "initSync", "parse", "survives", "validate", "whyNot"]);
const bytes = readFileSync(here("./pkg/pher_wasm_bg.wasm"));
pher.initSync({ module: bytes });
globalThis.fetch = realFetch;

function compareWithNative(rule, envelope) {
  const native = JSON.parse(
    execFileSync("cargo", ["run", "--quiet", "--locked", "-p", "pher-wasm", "--example", "native", "--", rule, envelope], {
      cwd: here("../.."),
      encoding: "utf8",
    }),
  );
  const wasm = {
    parse: JSON.parse(pher.parse(rule)),
    canon: pher.canon(rule),
    validate: pher.validate(rule) ?? null,
    evaluate: JSON.parse(pher.evaluate(rule, envelope)),
    survives: pher.survives(rule, envelope),
    whyNot: JSON.parse(pher.whyNot(rule, envelope)),
  };
  for (const name of Object.keys(native)) {
    assert.deepEqual(wasm[name], native[name], `${name}: wasm and native Rust disagree`);
  }
  return wasm;
}

const envelopeWith = (payload) =>
  JSON.stringify({
    id: "PH.01jdq3v8k2x9m4n7p0r5s6t8wz",
    ts: "2026-11-25T10:00:00Z",
    node: "n1",
    source: "hive",
    type: "hive.flag.set",
    subject: "hive.flag.set",
    payload,
  });
const rule = 'when on hive.flag.set where payload.flag.flag == "spawn_failed" then stream';

const rejected = compareWithNative(rule, envelopeWith({ flag: { flag: "other" } }));
assert.equal(rejected.survives, false);
assert.equal(rejected.whyNot.rejectedAt, "where");

const wasm = compareWithNative(rule, envelopeWith({ flag: { flag: "spawn_failed" } }));
assert.equal(wasm.evaluate.outcome, "matched");
assert.equal(wasm.whyNot.matched, true);
assert.equal(wasm.survives, true);
assert.equal(wasm.validate, null);

assert.equal(pher.validate("on hive.seal then cmd echo hi"), undefined);
assert.match(pher.validate("nonsense"), /on/);
assert.deepEqual(JSON.parse(pher.parse('on hive.seal where payload.status == "blocked" then buz CL.1')).on, ["hive.seal"]);
const blocked = 'on hive.seal where payload.status == "blocked" then cmd x';
assert.equal(pher.survives(blocked, JSON.stringify({ subject: "hive.seal", payload: { status: "blocked" } })), true);
assert.equal(pher.survives(blocked, JSON.stringify({ subject: "hive.seal", payload: { status: "done" } })), false);
assert.equal(pher.survives('on hive.seal meaning "stuck" then cmd x', JSON.stringify({ subject: "hive.seal" })), true);
const miss = JSON.parse(
  pher.whyNot("on hive.seal where payload.x == 1 then cmd x", JSON.stringify({ subject: "hive.seal", payload: { x: 2 } })),
);
assert.equal(miss.rejectedAt, "where");
assert.throws(() => pher.parse("nonsense"), /on/);

console.log(`@pheromone/wasm: glue clean of ${forbidden.map(([n]) => n).join(", ")}`);
console.log("@pheromone/wasm: initSync from bytes; parse, canon, validate, evaluate, survives, whyNot match native Rust");
console.log("@pheromone/wasm: all tests passed");
