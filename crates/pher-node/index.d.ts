/** Canonical JSON form of a subscription (see docs/LANGUAGE.md). */
export type SubscriptionJson = Record<string, unknown>;
/** Event object; at minimum `{ subject: string }` — other envelope fields are defaulted. */
export type EventInput = Record<string, unknown> & { subject: string };

/** Parse a subscription string into its canonical JSON form. Throws on parse errors. */
export function parse(subscription: string): SubscriptionJson;
/** Convert canonical JSON back to the canonical string form. */
export function fmt(json: SubscriptionJson): string;
/** Normalize a subscription string to its canonical form. */
export function canon(subscription: string): string;
/** Returns null when valid, else the parse error message (validate-as-you-type). */
export function validate(subscription: string): string | null;
/** Run one event through one subscription's tier 1-2 cascade. */
export function evaluate(subscription: string, event: EventInput): Record<string, unknown>;
/** Why did this event not match this subscription? First rejecting tier + reason. */
export function whyNot(subscription: string, event: EventInput): Record<string, unknown>;

/** Standing subscription set with the subject-trie prefilter, in-process. */
export class Matcher {
  constructor();
  insert(id: string, subscription: string): void;
  remove(id: string): boolean;
  matchIds(event: EventInput): string[];
  evaluate(event: EventInput): Array<Record<string, unknown>>;
  size(): number;
}
