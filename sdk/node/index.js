// @pheromone/client — talk to pherd from code.
//
// Two transports, one API:
//   - local:  the daemon's unix socket (PHEROMONE_HOME/pherd.sock)
//   - remote: a node's HTTP /rpc (cross-node; no streaming)
//
// `on()` registers a *connection-scoped* subscription (`then stream`): the
// daemon evaluates the full cascade server-side and pushes only deliveries
// down the socket. When the process exits or `close()` is called, the
// subscription is removed — the enforced form of `while <client> alive`.
// For durable push subscriptions (survive this process), use `when()` with a
// push sink (buz/hive/http/cmd/...).

import net from "node:net";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { EventEmitter } from "node:events";

function socketPath(home) {
  const base =
    home ?? process.env.PHEROMONE_HOME ?? path.join(os.homedir(), ".pheromone");
  return path.join(base, "pherd.sock");
}

/** One JSON line out, JSON lines in. */
class LineSocket extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.closed = false;
    let buf = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      buf += chunk;
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!line) continue;
        try {
          this.emit("line", JSON.parse(line));
        } catch (e) {
          this.emit("error", new Error(`bad line from pherd: ${e.message}`));
        }
      }
    });
    socket.on("close", () => {
      this.closed = true;
      this.emit("closed");
    });
    socket.on("error", (e) => this.emit("error", e));
  }

  static connect(sockPath) {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection(sockPath);
      socket.once("connect", () => {
        socket.removeAllListeners("error");
        resolve(new LineSocket(socket));
      });
      socket.once("error", (e) =>
        reject(
          new Error(
            `pherd is not running (cannot connect to ${sockPath}): ${e.message}`,
          ),
        ),
      );
    });
  }

  send(obj) {
    this.socket.write(JSON.stringify(obj) + "\n");
  }

  close() {
    this.closed = true;
    this.socket.destroy();
  }
}

/**
 * A live `on()` registration. EventEmitter: 'delivery', 'close', 'error',
 * 'reconnect'. `lastSeq` tracks the highest log seq seen; `ack` holds the
 * latest registration ack (resumedFrom, replayed, gapExpired, warnings).
 */
export class Listener extends EventEmitter {
  constructor() {
    super();
    this._sock = null;
    this.id = null;
    this.canonical = null;
    this.ack = null;
    this.lastSeq = 0;
    this.closedByUser = false;
  }

  /** Remove the subscription by hanging up (disables auto-reconnect). */
  close() {
    this.closedByUser = true;
    this._sock?.close();
  }
}

export class PherClient {
  constructor(transport) {
    this._t = transport;
  }

  /** Connect to the local daemon's unix socket. */
  static async connect({ home } = {}) {
    const sockPath = socketPath(home);
    const sock = await LineSocket.connect(sockPath);
    return new PherClient(new LocalTransport(sockPath, sock));
  }

  /** Target a remote node's HTTP /rpc (no streaming: `on`/`tail` throw). */
  static remote(url, { token } = {}) {
    return new PherClient(new RemoteTransport(url, token));
  }

  /** Emit an event onto the bus. */
  async emit(subject, payload, { type, source, correlation } = {}) {
    const event = { subject, payload: payload ?? null };
    if (type) event.type = type;
    if (source) event.source = source;
    if (correlation) event.correlation = correlation;
    const r = await this._t.call({ op: "emit", event });
    return { id: r.id, seq: r.seq };
  }

  /** Register a durable subscription (must use a push sink, not stream). */
  async when(subscription, { options = [], name } = {}) {
    const req = { op: "when", string: subscription, options };
    if (name) req.name = name;
    const r = await this._t.call(req);
    return {
      id: r.id,
      canonical: r.canonical,
      warnings: r.warnings ?? [],
      replayedDeliveries: r.replayedDeliveries ?? 0,
    };
  }

  /**
   * Register a connection-scoped subscription and receive deliveries as
   * callbacks. `then stream` is appended if the text has no then-clause.
   * The subscription is removed when the listener (or process) goes away.
   *
   * Robust consumption across flaky links:
   * - `cursor`: named hub-side position; resumes exactly where the last
   *   run committed, and (with `autoCommit`, default on) commits as
   *   deliveries are handled.
   * - `after`: one-shot resume from a log seq you tracked yourself.
   * - `reconnect`: on connection loss, re-attach with backoff and resume
   *   from the cursor / last seen seq ('reconnect' event fires).
   */
  async on(subscription, handler, opts = {}) {
    const {
      client,
      after,
      cursor,
      autoCommit = true,
      reconnect = false,
    } = opts;
    const text = /\sthen\s/.test(` ${subscription} `)
      ? subscription
      : `${subscription} then stream`;
    const listener = new Listener();
    if (after) listener.lastSeq = after;

    let commitTimer = null;
    let maxUncommitted = 0;
    const commitNow = () => {
      if (!cursor || !maxUncommitted) return;
      const seq = maxUncommitted;
      this._t
        .call({ op: "cursorCommit", name: cursor, seq })
        .catch((e) => listener.emit("error", e));
    };
    const scheduleCommit = (seq) => {
      maxUncommitted = Math.max(maxUncommitted, seq);
      if (commitTimer) return;
      commitTimer = setTimeout(() => {
        commitTimer = null;
        commitNow();
      }, 300);
      commitTimer.unref?.();
    };

    const onDelivery = (delivery) => {
      if (delivery.seq) {
        listener.lastSeq = Math.max(listener.lastSeq, delivery.seq);
        if (cursor && autoCommit) scheduleCommit(delivery.seq);
      }
      if (handler) handler(delivery);
      listener.emit("delivery", delivery);
    };

    const attach = async (resumeAfter) => {
      const req = {
        op: "listen",
        string: text,
        options: [],
        client: client ?? `sdk-${process.pid}`,
      };
      // The named cursor is authoritative when present; `after` covers
      // client-tracked resumes and first attach.
      if (cursor) req.cursor = cursor;
      if (!cursor && resumeAfter) req.after = resumeAfter;
      const sock = await this._t.stream(req);

      // The delivery handler must be wired BEFORE awaiting the ack: with
      // catch-up replay, deliveries can arrive in the same chunk as the ack
      // and would otherwise be dropped.
      let ackResolve, ackReject;
      const ackPromise = new Promise((res, rej) => {
        ackResolve = res;
        ackReject = rej;
      });
      let gotAck = false;
      sock.on("line", (line) => {
        if (!gotAck) {
          gotAck = true;
          ackResolve(line);
          return;
        }
        onDelivery(line);
      });
      sock.on("error", (e) => {
        if (!gotAck) ackReject(e);
        else listener.emit("error", e);
      });
      sock.on("closed", () => {
        if (!gotAck) {
          ackReject(new Error("pherd closed the connection before acking"));
          return;
        }
        if (commitTimer) {
          clearTimeout(commitTimer);
          commitTimer = null;
        }
        commitNow(); // flush the position before deciding what's next
        if (listener.closedByUser || !reconnect) {
          listener.emit("close");
          return;
        }
        reattachLoop();
      });

      const ack = await ackPromise;
      if (ack.ok !== true) {
        sock.close();
        throw new Error(ack.error ?? "listen failed");
      }
      listener._sock = sock;
      listener.id = ack.id;
      listener.canonical = ack.canonical;
      listener.ack = ack;
      return ack;
    };

    const reattachLoop = async () => {
      let delay = 1000;
      for (;;) {
        await new Promise((r) => setTimeout(r, delay).unref?.());
        if (listener.closedByUser) {
          listener.emit("close");
          return;
        }
        try {
          await attach(listener.lastSeq);
          listener.emit("reconnect", listener.id);
          return;
        } catch {
          delay = Math.min(delay * 2, 30_000);
        }
      }
    };

    await attach(listener.lastSeq);
    return listener;
  }

  /** Stream raw bus events (debugging surface, not delivery). */
  async tail({ after, subject } = {}, handler) {
    const sock = await this._t.stream({ op: "tail", after, subject });
    const emitter = new EventEmitter();
    sock.on("line", (line) => {
      if (line.ok === false) {
        emitter.emit("error", new Error(line.error));
        return;
      }
      if (handler) handler(line);
      emitter.emit("event", line);
    });
    sock.on("closed", () => emitter.emit("close"));
    emitter.close = () => sock.close();
    return emitter;
  }

  async ls() {
    return (await this._t.call({ op: "ls" })).subs;
  }

  /** Named cursors on the target node, plus the log head seq. */
  async cursors() {
    const r = await this._t.call({ op: "cursorLs" });
    return { cursors: r.cursors, head: r.head };
  }

  async cursorCommit(name, seq) {
    return (await this._t.call({ op: "cursorCommit", name, seq })).seq;
  }

  async cursorRm(name) {
    return (await this._t.call({ op: "cursorRm", name })).removed === true;
  }

  async rm(id) {
    return (await this._t.call({ op: "rm", id })).removed === true;
  }

  async status() {
    return this._t.call({ op: "status" });
  }

  async why(deliveryId) {
    return (await this._t.call({ op: "why", deliveryId })).delivery;
  }

  async whyNot(subId, eventId) {
    return (await this._t.call({ op: "whyNot", sub: subId, event: eventId }))
      .report;
  }

  /** Close the client's request connection (listeners close separately). */
  close() {
    this._t.close();
  }
}

/** Unix-socket transport: one shared request pipe + per-stream connections. */
class LocalTransport {
  constructor(sockPath, sock) {
    this.sockPath = sockPath;
    this._attach(sock);
  }

  _attach(sock) {
    this.sock = sock;
    this.queue = [];
    sock.on("line", (line) => {
      const pending = this.queue.shift();
      if (pending) pending.resolve(line);
    });
    sock.on("closed", () => {
      for (const p of this.queue.splice(0)) {
        p.reject(new Error("pherd closed the connection"));
      }
    });
  }

  async _ensure() {
    if (this.sock.closed) this._attach(await LineSocket.connect(this.sockPath));
    return this.sock;
  }

  async call(request) {
    const sock = await this._ensure();
    const response = await new Promise((resolve, reject) => {
      this.queue.push({ resolve, reject });
      sock.send(request);
    });
    if (response.ok !== true) {
      throw new Error(response.error ?? "unknown daemon error");
    }
    return response;
  }

  async stream(request) {
    const sock = await LineSocket.connect(this.sockPath);
    sock.send(request);
    return sock;
  }

  close() {
    this.sock.close();
  }
}

/** HTTP /rpc transport for remote nodes. Non-streaming ops only. */
class RemoteTransport {
  constructor(url, token) {
    this.url = url.replace(/\/+$/, "");
    this.token = token;
  }

  async call(request) {
    const headers = { "content-type": "application/json" };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    let res;
    try {
      res = await fetch(`${this.url}/rpc`, {
        method: "POST",
        headers,
        body: JSON.stringify(request),
      });
    } catch (e) {
      throw new Error(`remote node unreachable at ${this.url}: ${e.message}`);
    }
    const body = await res.json().catch(() => ({}));
    if (body.ok !== true) {
      throw new Error(body.error ?? `remote error (HTTP ${res.status})`);
    }
    return body;
  }

  /**
   * Remote streaming: POST /listen returns chunked NDJSON (ack line, then
   * deliveries; blank lines are heartbeats). Wrapped to look like a
   * LineSocket: 'line' / 'closed' / 'error' events and close().
   */
  async stream(request) {
    if (request.op !== "listen") {
      throw new Error(
        "only listen streams over HTTP — tail is a local debugging surface",
      );
    }
    const headers = { "content-type": "application/json" };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    const controller = new AbortController();
    let res;
    try {
      res = await fetch(`${this.url}/listen`, {
        method: "POST",
        headers,
        body: JSON.stringify(request),
        signal: controller.signal,
      });
    } catch (e) {
      throw new Error(`remote node unreachable at ${this.url}: ${e.message}`);
    }
    if (!res.ok && !res.body) {
      throw new Error(`remote listen failed (HTTP ${res.status})`);
    }

    const emitter = new EventEmitter();
    emitter.closed = false;
    emitter.close = () => {
      emitter.closed = true;
      controller.abort();
    };
    (async () => {
      let buf = "";
      try {
        for await (const chunk of res.body) {
          buf += Buffer.from(chunk).toString("utf8");
          let nl;
          while ((nl = buf.indexOf("\n")) >= 0) {
            const line = buf.slice(0, nl).trim();
            buf = buf.slice(nl + 1);
            if (!line) continue; // heartbeat
            try {
              emitter.emit("line", JSON.parse(line));
            } catch (e) {
              emitter.emit("error", new Error(`bad line: ${e.message}`));
            }
          }
        }
      } catch (e) {
        if (!emitter.closed) emitter.emit("error", e);
      } finally {
        emitter.closed = true;
        emitter.emit("closed");
      }
    })();
    return emitter;
  }

  close() {}
}
