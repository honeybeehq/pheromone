import { EventEmitter } from "node:events";

export interface Envelope {
  id: string;
  ts: string;
  node: string;
  source: string;
  type: string;
  subject: string;
  correlation?: string | null;
  payload: unknown;
  hops?: number;
}

export interface Delivery {
  deliveryId: string;
  event: Envelope;
  match: {
    subscription: string;
    tiers: string[];
    where?: unknown;
    meaning?: unknown;
    judge?: unknown;
    deliveryId?: string;
  };
  shaping?: unknown;
}

export interface EmitResult {
  id: string;
  seq: number;
  deliveries: number;
}

export interface WhenResult {
  id: string;
  canonical: string;
  warnings: string[];
  replayedDeliveries: number;
}

export interface SubInfo {
  id: string;
  string: string;
  json: unknown;
  created: string;
  expiresAt?: number;
  deliveries: number;
}

export declare class Listener extends EventEmitter {
  /** Subscription id (PH.xxxx). */
  readonly id: string;
  /** Canonical subscription text as registered. */
  readonly canonical: string;
  /** Hang up; the daemon removes the subscription. */
  close(): void;
  on(event: "delivery", cb: (delivery: Delivery) => void): this;
  on(event: "close", cb: () => void): this;
  on(event: "error", cb: (err: Error) => void): this;
}

export interface TailHandle extends EventEmitter {
  close(): void;
}

export declare class PherClient {
  /** Connect to the local daemon's unix socket (PHEROMONE_HOME/pherd.sock). */
  static connect(opts?: { home?: string }): Promise<PherClient>;
  /** Target a remote node over HTTP: /rpc for verbs, /listen for `on()`. */
  static remote(url: string, opts?: { token?: string }): PherClient;

  emit(
    subject: string,
    payload?: unknown,
    opts?: { type?: string; source?: string; correlation?: string },
  ): Promise<EmitResult>;

  /** Register a durable subscription with a push sink (buz/hive/http/cmd/...). */
  when(
    subscription: string,
    opts?: { options?: string[]; name?: string },
  ): Promise<WhenResult>;

  /**
   * Register a connection-scoped subscription (`then stream` appended if the
   * text has no then-clause) and receive deliveries as callbacks. The
   * subscription dies with the listener/process.
   */
  on(
    subscription: string,
    handler?: (delivery: Delivery) => void,
    opts?: { client?: string },
  ): Promise<Listener>;

  /** Stream raw bus events (debugging surface). */
  tail(
    opts?: { after?: number; subject?: string },
    handler?: (line: { seq: number; event: Envelope }) => void,
  ): Promise<TailHandle>;

  ls(): Promise<SubInfo[]>;
  rm(id: string): Promise<boolean>;
  status(): Promise<Record<string, unknown>>;
  why(deliveryId: string): Promise<unknown>;
  whyNot(subId: string, eventId: string): Promise<unknown>;
  close(): void;
}
