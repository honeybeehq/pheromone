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

/** A live `on()` registration. EventEmitter: 'delivery', 'close', 'error'. */
export class Listener extends EventEmitter {
  constructor(sock, id, canonical) {
    super();
    this._sock = sock;
    this.id = id;
    this.canonical = canonical;
  }

  /** Remove the subscription by hanging up. */
  close() {
    this._sock.close();
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
    return { id: r.id, seq: r.seq, deliveries: r.deliveries };
  }

  /** Register a durable subscription (must use a push sink, not stream). */
  async when(subscription, { options = [] } = {}) {
    const r = await this._t.call({ op: "when", string: subscription, options });
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
   */
  async on(subscription, handler, { client } = {}) {
    const text = /\sthen\s/.test(` ${subscription} `)
      ? subscription
      : `${subscription} then stream`;
    const sock = await this._t.stream({
      op: "listen",
      string: text,
      options: [],
      client: client ?? `sdk-${process.pid}`,
    });
    const ack = await new Promise((resolve, reject) => {
      sock.once("line", resolve);
      sock.once("closed", () =>
        reject(new Error("pherd closed the connection before acking")),
      );
      sock.once("error", reject);
    });
    if (ack.ok !== true) {
      sock.close();
      throw new Error(ack.error ?? "listen failed");
    }
    const listener = new Listener(sock, ack.id, ack.canonical);
    sock.on("line", (delivery) => {
      if (handler) handler(delivery);
      listener.emit("delivery", delivery);
    });
    sock.on("closed", () => listener.emit("close"));
    sock.on("error", (e) => listener.emit("error", e));
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

  stream() {
    throw new Error(
      "streaming (on/tail) is not supported over HTTP /rpc — connect an SDK " +
        "client on the node itself, or use a push sink (`when` + then http/buz/...)",
    );
  }

  close() {}
}
