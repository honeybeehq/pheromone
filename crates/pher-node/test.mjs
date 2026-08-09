// Smoke test for the napi addon: node test.mjs
import assert from "node:assert/strict";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const pher = require("./index.js");

// parse → canonical JSON
const js = pher.parse(
  'on ci.github.run.completed where payload.conclusion == "failure" then cmd echo hit',
);
assert.deepEqual(js.on, ["ci.github.run.completed"]);
assert.equal(js.where, 'payload.conclusion == "failure"');
assert.equal(js.then.sink, "cmd");

// fmt round-trip
const str = pher.fmt(js);
assert.equal(str, pher.canon(str));

// validate-as-you-type
assert.equal(pher.validate("on hive.seal then cmd echo hi"), null);
assert.match(pher.validate("where payload.x == 1 then cmd hi"), /on/);

// evaluate + whyNot
const hit = pher.evaluate(
  'on hive.seal where payload.status == "blocked" then cmd echo hi',
  { subject: "hive.seal", payload: { status: "blocked" } },
);
assert.equal(hit.outcome, "matched");
assert.deepEqual(hit.match.tiers, ["on", "where"]);

const miss = pher.whyNot(
  'on hive.seal where payload.status == "blocked" then cmd echo hi',
  { subject: "hive.seal", payload: { status: "done" } },
);
assert.equal(miss.matched, false);
assert.equal(miss.rejectedAt, "where");

// standing Matcher
const m = new pher.Matcher();
m.insert("A", "on hive.** then cmd echo a");
m.insert("B", 'on pol.job.* where payload.ok == false then cmd echo b');
assert.equal(m.size(), 2);
assert.deepEqual(m.matchIds({ subject: "hive.seal" }), ["A"]);
assert.deepEqual(m.matchIds({ subject: "pol.job.done", payload: { ok: false } }), ["B"]);
assert.deepEqual(m.matchIds({ subject: "pol.job.done", payload: { ok: true } }), []);
assert.equal(m.remove("A"), true);
assert.deepEqual(m.matchIds({ subject: "hive.seal" }), []);

console.log("@pheromone/core: all smoke tests passed");
