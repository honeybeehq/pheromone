// WASM smoke test: the same matcher, in a wasm host.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const pher = require("./pkg/pher_wasm.js");

assert.equal(pher.validate("on hive.seal then cmd echo hi"), undefined);
assert.match(pher.validate("nonsense"), /on/);

const js = JSON.parse(pher.parse('on hive.seal where payload.status == "blocked" then buz CL.1'));
assert.deepEqual(js.on, ["hive.seal"]);

// filter-at-source: only survivors cross the wire
assert.equal(
  pher.survives('on hive.seal where payload.status == "blocked" then cmd x',
    JSON.stringify({ subject: "hive.seal", payload: { status: "blocked" } })), true);
assert.equal(
  pher.survives('on hive.seal where payload.status == "blocked" then cmd x',
    JSON.stringify({ subject: "hive.seal", payload: { status: "done" } })), false);
// meaning clause = pending = survives (tier 3 runs upstream)
assert.equal(
  pher.survives('on hive.seal meaning "stuck" then cmd x',
    JSON.stringify({ subject: "hive.seal" })), true);

const miss = JSON.parse(pher.whyNot('on hive.seal where payload.x == 1 then cmd x',
  JSON.stringify({ subject: "hive.seal", payload: { x: 2 } })));
assert.equal(miss.rejectedAt, "where");

console.log("@pheromone/wasm: all smoke tests passed");
