// Live test: spins up a real pherd in a temp home and drives it through the
// SDK. Requires a debug build (cargo build -p pher).
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PherClient } from "./index.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const bin = path.join(root, "target/debug/pher");
const home = fs.mkdtempSync("/tmp/pher-sdk-test-");

if (!fs.existsSync(bin)) {
  console.error(`missing ${bin} — run: cargo build -p pher`);
  process.exit(1);
}

const HTTP = "127.0.0.1:14877";
const TOKEN = "sdk-test-token";
const daemon = spawn(bin, ["daemon", "run"], {
  env: {
    ...process.env,
    PHEROMONE_HOME: home,
    PHER_EMBED: "off",
    PHER_HTTP: HTTP,
    PHER_HTTP_TOKEN: TOKEN,
  },
  stdio: ["ignore", "ignore", "inherit"],
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitForSocket() {
  for (let i = 0; i < 50; i++) {
    if (fs.existsSync(path.join(home, "pherd.sock"))) return;
    await sleep(100);
  }
  throw new Error("pherd socket never appeared");
}

/** Poll until fn() is truthy or time runs out. */
async function until(fn, what, ms = 3000) {
  const t0 = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - t0 > ms) throw new Error(`timed out waiting for ${what}`);
    await sleep(50);
  }
}

let failed = false;
try {
  await waitForSocket();
  const pher = await PherClient.connect({ home });

  // RPC surface.
  const status = await pher.status();
  assert.equal(status.subscriptions, 0);
  console.log("ok  status");

  // Durable registration still works, and stream is refused for `when`.
  const durable = await pher.when("on sdk.other.* then cmd true");
  assert.match(durable.id, /^PH\./);
  await assert.rejects(
    () => pher.when("on sdk.x then stream"),
    /live listener connection/,
  );
  console.log("ok  when + stream gate");

  // Connection-scoped listener: full cascade server-side, callbacks here.
  const got = [];
  const listener = await pher.on(
    'on sdk.test.* where payload.kind == "boom"',
    (d) => got.push(d),
  );
  assert.match(listener.id, /^PH\./);
  assert.match(listener.canonical, /then stream while sdk-\d+ alive/);

  await pher.emit("sdk.test.run", { kind: "boom", n: 1 });
  await pher.emit("sdk.test.run", { kind: "calm", n: 2 }); // filtered server-side
  await pher.emit("sdk.other.run", { kind: "boom" }); // wrong subject
  await until(() => got.length >= 1, "delivery");
  await sleep(200);
  assert.equal(got.length, 1);
  assert.equal(got[0].event.payload.kind, "boom");
  assert.equal(got[0].match.subscription, listener.id);
  assert.ok(got[0].deliveryId.startsWith(listener.id));
  console.log("ok  on() delivers matches only");

  // Disconnect removes the subscription (enforced lease).
  listener.close();
  await until(
    async () => !(await pher.ls()).some((s) => s.id === listener.id),
    "listener sub removal",
  );
  console.log("ok  close() evaporates the subscription");

  // limit composes: daemon retires the sub and hangs up on us.
  const once = await pher.on("on sdk.once then stream limit 1");
  const closed = new Promise((r) => once.once("close", r));
  const first = new Promise((r) => once.once("delivery", r));
  await pher.emit("sdk.once", { n: 1 });
  await first;
  await closed;
  assert.equal((await pher.ls()).some((s) => s.id === once.id), false);
  console.log("ok  limit 1 retires and hangs up");

  // why-based introspection through the SDK.
  const why = await pher.why(got[0].deliveryId);
  assert.equal(why.match.subscription, listener.id);
  console.log("ok  why()");

  // Remote transport over HTTP: verbs via /rpc, streaming via /listen.
  const hub = PherClient.remote(`http://${HTTP}`, { token: TOKEN });
  const hubStatus = await hub.status();
  assert.equal(hubStatus.ok, true);
  const remoteGot = [];
  const remoteListener = await hub.on(
    'on mesh.* where payload.sev == "high"',
    (d) => remoteGot.push(d),
  );
  assert.match(remoteListener.id, /^PH\./);
  await hub.emit("mesh.alert", { sev: "high" });
  await hub.emit("mesh.alert", { sev: "low" });
  await until(() => remoteGot.length >= 1, "remote delivery");
  await sleep(200);
  assert.equal(remoteGot.length, 1);
  assert.equal(remoteGot[0].event.payload.sev, "high");
  console.log("ok  remote on() over HTTP /listen");

  // Remote hangup: detection is write-bounded (TCP needs a second write
  // after the peer vanishes to surface the error, or the 15s heartbeat), so
  // keep emitting while polling.
  remoteListener.close();
  await until(async () => {
    await hub.emit("mesh.alert", { sev: "high" });
    return !(await hub.ls()).some((s) => s.id === remoteListener.id);
  }, "remote listener teardown");
  console.log("ok  remote close() evaporates the subscription");

  // Cursor-based resume: exactly the gap, no duplicates, across a hangup.
  const got1 = [];
  const c1 = await pher.on("on cur.*", (d) => got1.push(d), { cursor: "t-cur" });
  await pher.emit("cur.a", { n: 1 });
  await until(() => got1.length >= 1, "cursor delivery 1");
  assert.ok(got1[0].seq > 0, "deliveries carry seq");
  await sleep(500); // let autoCommit's debounce flush
  c1.close();
  await until(
    async () => !(await pher.ls()).some((s) => s.id === c1.id),
    "cursor listener teardown",
  );
  await pher.emit("cur.b", { n: 2 });
  await pher.emit("cur.c", { n: 3 });
  const got2 = [];
  const c2 = await pher.on("on cur.*", (d) => got2.push(d), { cursor: "t-cur" });
  assert.ok(c2.ack.resumedFrom >= got1[0].seq, "ack reports resume point");
  await until(() => got2.length >= 2, "cursor gap replay");
  await sleep(200);
  assert.deepEqual(
    got2.map((d) => d.event.subject),
    ["cur.b", "cur.c"],
    "exactly the missed events, in order",
  );
  c2.close();
  const { cursors } = await pher.cursors();
  assert.ok(cursors.some((c) => c.name === "t-cur"));
  console.log("ok  cursor resume replays exactly the gap");

  // Reconnect: a server-side hangup (rm) triggers re-attach with backoff.
  const got3 = [];
  const r1 = await pher.on("on rec.*", (d) => got3.push(d), { reconnect: true });
  const firstId = r1.id;
  const reconnected = new Promise((r) => r1.once("reconnect", r));
  await pher.rm(firstId);
  await reconnected;
  assert.notEqual(r1.id, firstId, "re-attached under a new sub id");
  await pher.emit("rec.x", { n: 1 });
  await until(() => got3.length >= 1, "delivery after reconnect");
  r1.close();
  console.log("ok  reconnect re-attaches after server-side hangup");

  // Auth and reachability failures stay useful.
  const badAuth = PherClient.remote(`http://${HTTP}`, { token: "wrong" });
  await assert.rejects(() => badAuth.status(), /unauthorized/);
  const unreachable = PherClient.remote("http://127.0.0.1:1", { token: "x" });
  await assert.rejects(() => unreachable.status(), /unreachable/);
  await assert.rejects(() => unreachable.on("on x", () => {}), /unreachable/);
  console.log("ok  remote transport errors");

  pher.close();
  console.log("\nALL PASS");
} catch (e) {
  failed = true;
  console.error("FAIL:", e);
} finally {
  daemon.kill();
  fs.rmSync(home, { recursive: true, force: true });
}
process.exit(failed ? 1 : 0);
