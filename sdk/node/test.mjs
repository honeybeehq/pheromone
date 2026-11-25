// Live test: spins up a real pherd in a temp home and drives it through the
// SDK. Requires a debug build (cargo build -p pher).
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
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

const judgeCalls = [];
const judgeServer = http.createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    const summary = JSON.parse(body).messages[0].content;
    judgeCalls.push(summary);
    const verdict = summary.includes("bad");
    res.setHeader("content-type", "application/json");
    setTimeout(() => res.end(
      JSON.stringify({
        content: [{ type: "text", text: JSON.stringify({ verdict, rationale: "fake judge" }) }],
      }),
    ), summary.includes("slow") ? 800 : 0);
  });
});
await new Promise((r) => judgeServer.listen(0, "127.0.0.1", r));

const HTTP = "127.0.0.1:14877";
const TOKEN = "sdk-test-token";
const daemon = spawn(bin, ["daemon", "run"], {
  env: {
    ...process.env,
    PHEROMONE_HOME: home,
    PHER_EMBED: "off",
    PHER_HTTP: HTTP,
    PHER_HTTP_TOKEN: TOKEN,
    ANTHROPIC_API_KEY: "fake-judge-key",
    PHER_JUDGE_MODEL: "claude-haiku-4-5",
    PHER_JUDGE_BASE_URL: `http://127.0.0.1:${judgeServer.address().port}`,
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

  await pher.cursorCommit("t-hang", (await pher.cursors()).head);
  const neverSettles = new Promise(() => {});
  const handled = [];
  const h1 = await pher.on(
    "on hang.*",
    (d) => {
      handled.push(d.event.subject);
      return d.event.subject === "hang.a" ? neverSettles : sleep(10);
    },
    { cursor: "t-hang" },
  );
  await pher.emit("hang.a", { n: 1 });
  await pher.emit("hang.b", { n: 2 });
  await until(() => handled.length >= 2, "both hang deliveries handled");
  await sleep(500);
  h1.close();
  await until(
    async () => !(await pher.ls()).some((s) => s.id === h1.id),
    "hang listener teardown",
  );
  const redelivered = [];
  const h2 = await pher.on("on hang.*", (d) => redelivered.push(d), {
    cursor: "t-hang",
  });
  await until(() => redelivered.length >= 2, "unfinished delivery replay");
  await sleep(200);
  assert.deepEqual(
    redelivered.map((d) => d.event.subject),
    ["hang.a", "hang.b"],
    "an unfinished handler holds the cursor before its seq",
  );
  h2.close();
  console.log("ok  cursor waits for unfinished handlers");

  await pher.cursorCommit("t-fail", (await pher.cursors()).head);
  const handlerErrors = [];
  const f1 = await pher.on(
    "on fail.*",
    async (d) => {
      if (d.event.subject === "fail.a") throw new Error("handler blew up");
    },
    { cursor: "t-fail" },
  );
  f1.on("error", (e) => handlerErrors.push(e));
  await pher.emit("fail.a", { n: 1 });
  await pher.emit("fail.b", { n: 2 });
  await until(() => handlerErrors.length >= 1, "handler rejection reported");
  assert.equal(handlerErrors[0].message, "handler blew up");
  assert.equal(handlerErrors[0].delivery.event.subject, "fail.a");
  await sleep(500);
  f1.close();
  await until(
    async () => !(await pher.ls()).some((s) => s.id === f1.id),
    "fail listener teardown",
  );
  const retried = [];
  const f2 = await pher.on("on fail.*", (d) => retried.push(d), {
    cursor: "t-fail",
  });
  await until(() => retried.length >= 2, "rejected delivery replay");
  await sleep(200);
  assert.deepEqual(
    retried.map((d) => d.event.subject),
    ["fail.a", "fail.b"],
    "a rejected handler is redelivered on resume",
  );
  await sleep(500);
  f2.close();
  const after = (await pher.cursors()).cursors.find((c) => c.name === "t-fail");
  assert.equal(after.seq, retried[1].seq, "cursor advances once handlers succeed");
  console.log("ok  rejected handler is redelivered on resume");

  await pher.cursorCommit("t-judge", (await pher.cursors()).head);
  const judgeSub = 'on jdg.* judge "is it bad?" budget 3/day';
  const away = [];
  for (const [subject, kind] of [
    ["jdg.a", "bad-1"],
    ["jdg.b", "fine"],
    ["jdg.c", "bad-2"],
    ["jdg.d", "bad-3"],
  ]) {
    away.push((await pher.emit(subject, { kind })).seq);
  }
  const judged = [];
  const gaps = [];
  const j1 = await pher.on(judgeSub, (d) => judged.push(d), {
    cursor: "t-judge",
    onGap: (gap) => gaps.push(gap),
  });
  await until(() => gaps.length >= 1, "catch-up gap after judge budget runs out", 10000);
  await sleep(200);
  assert.deepEqual(
    judged.map((d) => d.event.payload.kind),
    ["bad-1", "bad-2"],
    "catch-up judges the missed events and delivers the yes verdicts",
  );
  assert.deepEqual(judged[0].match.tiers.slice(-1), ["judge"]);
  assert.equal(judged[0].match.judge.rationale, "fake judge");
  assert.deepEqual(
    { from: gaps[0].from, reason: gaps[0].reason, subscription: gaps[0].subscription },
    { from: away[3], reason: "budgetExhausted", subscription: j1.id },
    "budget exhaustion is an explicit gap from the first unjudged seq",
  );
  assert.ok(gaps[0].through >= away[3]);
  assert.equal(judgeCalls.length, 3, "judge calls stop at the budget");
  await sleep(500);
  j1.close();
  await until(
    async () => !(await pher.ls()).some((s) => s.id === j1.id),
    "judge listener teardown",
  );
  const judgeCursor = (await pher.cursors()).cursors.find((c) => c.name === "t-judge");
  assert.equal(judgeCursor.seq, gaps[0].through, "a handled gap commits the cursor");
  console.log("ok  judge listener catch-up judges the gap, budget exhaustion is explicit");

  await pher.cursorCommit("t-judge-unhandled", (await pher.cursors()).head);
  const unhandledFrom = [];
  for (const kind of ["bad-4", "bad-5", "bad-6", "bad-7"]) {
    unhandledFrom.push((await pher.emit("jdg.e", { kind: `${kind}-${Date.now()}` })).seq);
  }
  const gapErrors = [];
  const unhandled = [];
  const j2 = await pher.on(judgeSub, (d) => unhandled.push(d), {
    cursor: "t-judge-unhandled",
  });
  j2.on("error", (e) => gapErrors.push(e));
  await until(() => gapErrors.length >= 1, "unhandled gap surfaces as an error", 10000);
  assert.equal(gapErrors[0].gap.reason, "budgetExhausted");
  assert.equal(gapErrors[0].gap.from, unhandledFrom[3]);
  assert.equal(unhandled.length, 3);
  await sleep(500);
  j2.close();
  await until(
    async () => !(await pher.ls()).some((s) => s.id === j2.id),
    "unhandled judge listener teardown",
  );
  const stuck = (await pher.cursors()).cursors.find((c) => c.name === "t-judge-unhandled");
  assert.equal(
    stuck.seq,
    gapErrors[0].gap.from - 1,
    "an unhandled gap commits up to, never past, the gap",
  );
  console.log("ok  unhandled catch-up gap is an error and holds the cursor");

  const callsBeforeRetry = judgeCalls.length;
  const retriedGaps = [];
  const rejudged = [];
  const j3 = await pher.on(judgeSub, (d) => rejudged.push(d), {
    cursor: "t-judge-unhandled",
    onGap: (gap) => retriedGaps.push(gap),
  });
  await until(() => retriedGaps.length >= 1, "retried gap", 10000);
  await sleep(200);
  assert.equal(judgeCalls.length, callsBeforeRetry, "reattach reuses recorded verdicts");
  assert.deepEqual(
    rejudged.map((d) => [d.seq, d.match.judge.cached]),
    unhandledFrom
      .slice(0, 3)
      .filter((seq) => seq > stuck.seq)
      .map((seq) => [seq, true]),
  );
  assert.equal(retriedGaps[0].from, unhandledFrom[3], "budget is shared across reattaches");
  j3.close();
  console.log("ok  reattach reuses recorded verdicts and the same judge budget");

  const raceSub = 'on race.* judge "is it bad?" budget 50/day';
  await pher.cursorCommit("t-race", (await pher.cursors()).head);
  const warm = [];
  const w1 = await pher.on(raceSub, (d) => warm.push(d), { cursor: "t-race" });
  await pher.emit("race.x", { kind: "bad dup" });
  await until(() => warm.length >= 1, "warm verdict cache");
  await sleep(500);
  w1.close();
  await until(async () => !(await pher.ls()).some((s) => s.id === w1.id), "warm teardown");
  const backlog = (await pher.emit("race.x", { kind: "bad slow backlog" })).seq;
  const raced = [];
  const r2 = await pher.on(raceSub, (d) => raced.push(d), { cursor: "t-race" });
  const slowLive = (await pher.emit("race.x", { kind: "bad slow live" })).seq;
  const dup = (await pher.emit("race.x", { kind: "bad dup" })).seq;
  await until(() => raced.length >= 3, "catch-up race deliveries", 10000);
  await sleep(300);
  assert.deepEqual(
    raced.map((d) => d.seq),
    [backlog, slowLive, dup],
    "a cached verdict never overtakes an earlier pending judge during catch-up",
  );
  r2.close();
  console.log("ok  catch-up release keeps judge deliveries in seq order");

  const liveRaced = [];
  const r3 = await pher.on(raceSub, (d) => liveRaced.push(d), { cursor: "t-race-live" });
  await pher.emit("race.y", { kind: "bad dup live" });
  await until(() => liveRaced.length >= 1, "warm live verdict");
  const slow2 = (await pher.emit("race.y", { kind: "bad slow live 2" })).seq;
  const dup2 = (await pher.emit("race.y", { kind: "bad dup live" })).seq;
  await until(() => liveRaced.length >= 3, "live race deliveries", 10000);
  await sleep(300);
  assert.deepEqual(
    liveRaced.slice(1).map((d) => d.seq),
    [slow2, dup2],
    "a cached live verdict waits for the earlier pending judge",
  );
  r3.close();
  console.log("ok  live judge deliveries stay in seq order");

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
  judgeServer.close();
  fs.rmSync(home, { recursive: true, force: true });
}
process.exit(failed ? 1 : 0);
