use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use pher_core::matcher::{evaluate, why_not, MatchBlock, Outcome};
use pher_core::{ids, Envelope, Matcher, Sink, SubjectPattern, Subscription};

use crate::protocol::{err, ok, PartialEvent, Request};
use crate::store::{write_json_atomic, Paths};

const DEFAULT_RETENTION_SECS: u64 = 7 * 86400;
const MAX_EMIT_HOPS: u32 = 8;

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn now_ts() -> String {
    humantime::format_rfc3339_millis(SystemTime::now()).to_string()
}

fn unix_to_ts(secs: u64) -> String {
    humantime::format_rfc3339_millis(UNIX_EPOCH + Duration::from_secs(secs)).to_string()
}

/// Persisted registration record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubMeta {
    pub id: String,
    /// Stable name for declarative reconciliation (`pher apply`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub name: Option<String>,
    /// Canonical string form.
    pub string: String,
    /// Canonical JSON form (source of truth for reconstruction).
    pub json: Value,
    pub created: String,
    #[serde(rename = "expiresAt", skip_serializing_if = "Option::is_none", default)]
    pub expires_at: Option<u64>,
    pub deliveries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Timer {
    #[serde(rename = "subId")]
    sub_id: String,
    origin: Envelope,
    /// Unix seconds.
    deadline: u64,
    /// Log seq of the origin event (cursor bookkeeping for the delivery).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
}

/// A named consumer position in this node's log (`listen --cursor <name>`).
/// Client-committed: the hub never guesses what a consumer has processed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorEntry {
    pub seq: u64,
    /// Unix secs of the last commit — evaporation clock.
    pub ts: u64,
}

struct TailClient {
    tx: mpsc::Sender<String>,
    subject: Option<SubjectPattern>,
}

/// An open delivery-shaping window for one subscription (`every`/`batch`).
/// Persisted so kill -9 loses at most the current window's timing, never
/// its queued events.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingWindow {
    mode: String, // "debounce" | "batch"
    #[serde(rename = "windowEnd")]
    window_end: u64,
    events: Vec<Envelope>,
    /// Log seqs parallel to `events` (cursor bookkeeping at window close).
    #[serde(default)]
    seqs: Vec<u64>,
    /// Events collapsed (debounce) or dropped over the queue cap (batch).
    #[serde(default)]
    collapsed: u64,
}

/// Evaporation bound for batch queues (principle 6: no unbounded buffers).
const MAX_BATCH_QUEUE: usize = 1000;

pub(crate) struct State {
    paths: Paths,
    node: String,
    matcher: Matcher,
    metas: HashMap<String, SubMeta>,
    next_seq: u64,
    timers: Vec<Timer>,
    pending: HashMap<String, PendingWindow>,
    tails: Vec<TailClient>,
    /// Live `then stream` listeners by subscription id. Dropping the sender
    /// ends the listener's delivery loop; a vanished listener removes the
    /// subscription — the enforced form of `while <client> alive`.
    listeners: HashMap<String, ListenerHandle>,
    /// Named consumer cursors (client-committed positions in this log).
    cursors: HashMap<String, CursorEntry>,
    /// Named token grants (authorization = the subscription language).
    pub(crate) grants: HashMap<String, GrantRuntime>,
    /// Bridge definitions (pull composition from upstream buses).
    bridges: HashMap<String, crate::bridge::BridgeDef>,
    /// Cancel flags for running bridge workers.
    bridge_cancels: HashMap<String, Arc<std::sync::atomic::AtomicBool>>,
    /// Log seq of the event currently being delivered — stamped into
    /// delivery records and stream lines so consumers can track a cursor.
    /// Every deliver() path sets it first (ingest, replay, timers, windows,
    /// async judge verdicts), so it is never stale at delivery time.
    current_seq: Option<u64>,
    retention_secs: u64,
    semantic: crate::semantic::Semantic,
    judge_cfg: Option<crate::judge::JudgeConfig>,
    judge_tx: Option<mpsc::Sender<JudgeJob>>,
    judge_budgets: HashMap<String, JudgeBudget>,
    verdict_cache: HashMap<String, crate::judge::Verdict>,
    conditions: crate::conditions::Conditions,
    /// Admission dedup for cross-node deliveries (Connected-store pattern):
    /// at-least-once shipping, effectively-once ingestion. Persisted to
    /// forwarded_seen.jsonl so restarts don't reopen the window.
    forwarded_seen: std::collections::VecDeque<String>,
    /// Pending outbound HTTP deliveries (persisted; retried with backoff).
    outbox: Vec<OutboxEntry>,
    /// Nudges the outbox worker after an enqueue (fallback: 1s tick).
    outbox_tx: Option<mpsc::Sender<()>>,
}

/// Evaporation bound for the forwarded-delivery dedup window.
const MAX_FORWARDED_SEEN: usize = 50_000;

/// Per-subscription judge budget window. Persisted; fail-closed on exhaustion.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct JudgeBudget {
    #[serde(rename = "periodStart")]
    period_start: u64,
    used: u64,
    #[serde(default)]
    notified: bool,
}

/// A queued outbound HTTP delivery. The http sink never fire-and-forgets:
/// every send goes through the outbox, which retries with backoff until the
/// receiver accepts it or the entry outlives retention. Cross-node dedup on
/// the receiving side makes retries safe (at-least-once shipping,
/// effectively-once ingestion).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutboxEntry {
    #[serde(rename = "deliveryId")]
    delivery_id: String,
    method: String,
    url: String,
    body: String,
    #[serde(default)]
    attempts: u32,
    /// Unix secs of the next attempt (0 = due immediately).
    #[serde(rename = "nextTry", default)]
    next_try: u64,
    created: u64,
}

/// Retry backoff: 2s, 4s, 8s, … capped at 15 minutes.
fn outbox_backoff(attempts: u32) -> u64 {
    (2u64 << attempts.min(12)).min(900)
}

/// A named bearer token whose surface is a pair of subscription-language
/// filters: `allow` bounds what it may consume, `emit` what it may publish.
/// Authorization IS the matcher — no second policy language.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GrantDef {
    pub name: String,
    pub token: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub allow: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub emit: Option<String>,
}

pub(crate) struct GrantRuntime {
    pub def: GrantDef,
    pub allow_sub: Option<Subscription>,
    pub emit_sub: Option<Subscription>,
}

impl GrantDef {
    pub fn compile(self) -> Result<GrantRuntime, String> {
        let allow_sub = self.allow.as_deref().map(parse_filter).transpose()?;
        let emit_sub = self.emit.as_deref().map(parse_filter).transpose()?;
        if allow_sub.is_none() && emit_sub.is_none() {
            return Err(format!(
                "grant '{}' needs at least one of allow/emit",
                self.name
            ));
        }
        Ok(GrantRuntime {
            def: self,
            allow_sub,
            emit_sub,
        })
    }

    /// First 12 hex chars of sha256(token): diffable identity that never
    /// exposes the token (`pher grant ls`, `pher apply`).
    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(self.token.as_bytes()))[..12].to_string()
    }
}

/// `on <subjects> [from <node>] [where <expr>]` — a deterministic filter in
/// the subscription language. Tiers 1-2 only: authorization never consults
/// an embedding or an LLM.
pub(crate) fn parse_filter(text: &str) -> Result<Subscription, String> {
    let sub = Subscription::parse(&format!("{text} then stream")).map_err(|e| {
        format!("filter '{text}': {e} (filters are 'on <subjects> [where <expr>]', no then-clause)")
    })?;
    if sub.meaning.is_some() || sub.judge.is_some() || sub.expect.is_some() {
        return Err(format!(
            "filter '{text}': grant filters are deterministic — tiers 1-2 only"
        ));
    }
    // The action clause swallows rest-of-line, so an embedded `then …` in the
    // filter text would otherwise parse "successfully" as sink arguments.
    if sub.then.sink != Sink::Stream || !sub.then.args.is_empty() {
        return Err(format!(
            "filter '{text}': filters take no then-clause — they bound, they don't act"
        ));
    }
    Ok(sub)
}

pub(crate) fn filter_matches(filter: &Subscription, event: &Envelope) -> bool {
    matches!(
        evaluate("GRANT", filter, event, None).outcome,
        Outcome::Matched
    )
}

/// A live stream listener: its delivery channel, plus the grant filter it
/// is bounded by (None = local/admin, unfiltered).
pub(crate) struct ListenerHandle {
    tx: mpsc::Sender<String>,
    filter: Option<Subscription>,
}

/// A pending tier-4 evaluation, processed off-lock by the judge worker.
struct JudgeJob {
    sub_id: String,
    event: Envelope,
    /// Log seq of the event (verdict deliveries carry it for cursors).
    seq: Option<u64>,
    summary: String,
    cache_key: String,
    question: String,
    meaning_record: Option<Value>,
    config: crate::judge::JudgeConfig,
}

impl State {
    fn load(paths: Paths) -> anyhow::Result<State> {
        paths.ensure()?;
        let node = hostname();
        let retention_secs = std::env::var("PHER_RETENTION")
            .ok()
            .and_then(|s| pher_core::Dur::parse(&s).ok())
            .map(|d| d.secs())
            .unwrap_or(DEFAULT_RETENTION_SECS);

        let mut matcher = Matcher::new();
        let mut metas = HashMap::new();
        if let Ok(text) = std::fs::read_to_string(paths.subs()) {
            let list: Vec<SubMeta> = serde_json::from_str(&text).context("subs.json corrupt")?;
            let now = now_unix();
            for meta in list {
                if meta.expires_at.is_some_and(|t| t <= now) {
                    continue; // evaporated while we were down
                }
                let sub = Subscription::from_json(&meta.json)
                    .with_context(|| format!("subscription {} corrupt", meta.id))?;
                if sub.then.sink == Sink::Stream {
                    continue; // its listener connection died with the old process
                }
                matcher.insert(meta.id.clone(), sub);
                metas.insert(meta.id.clone(), meta);
            }
        }

        let next_seq = last_seq(&paths).map(|s| s + 1).unwrap_or(1);

        let timers: Vec<Timer> = std::fs::read_to_string(paths.timers())
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default();

        let pending: HashMap<String, PendingWindow> = std::fs::read_to_string(paths.pending())
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default();

        let judge_budgets: HashMap<String, JudgeBudget> =
            std::fs::read_to_string(paths.judge_budgets())
                .ok()
                .and_then(|t| serde_json::from_str(&t).ok())
                .unwrap_or_default();
        let mut verdict_cache: HashMap<String, crate::judge::Verdict> = HashMap::new();
        if let Ok(text) = std::fs::read_to_string(paths.verdicts()) {
            for line in text.lines() {
                let Ok(v) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                if let (Some(key), Some(verdict)) = (
                    v.get("key").and_then(|k| k.as_str()),
                    v.get("verdict").and_then(|b| b.as_bool()),
                ) {
                    verdict_cache.insert(
                        key.to_string(),
                        crate::judge::Verdict {
                            verdict,
                            rationale: v
                                .get("rationale")
                                .and_then(|r| r.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        },
                    );
                }
            }
        }

        let conditions = crate::conditions::Conditions::from_json(
            std::fs::read_to_string(paths.conditions())
                .ok()
                .and_then(|t| serde_json::from_str(&t).ok())
                .unwrap_or(Value::Null),
            std::fs::read_to_string(paths.condition_state())
                .ok()
                .and_then(|t| serde_json::from_str(&t).ok())
                .unwrap_or(Value::Null),
        );

        let cursors: HashMap<String, CursorEntry> = std::fs::read_to_string(paths.cursors())
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default();

        // Rehydrate the forwarding dedup window (last MAX entries).
        let mut forwarded_seen = std::collections::VecDeque::new();
        if let Ok(text) = std::fs::read_to_string(paths.forwarded()) {
            for line in text.lines() {
                if let Some(id) = serde_json::from_str::<Value>(line)
                    .ok()
                    .and_then(|v| v.get("id").and_then(|i| i.as_str()).map(String::from))
                {
                    forwarded_seen.push_back(id);
                    if forwarded_seen.len() > MAX_FORWARDED_SEEN {
                        forwarded_seen.pop_front();
                    }
                }
            }
        }

        let outbox: Vec<OutboxEntry> = std::fs::read_to_string(paths.outbox())
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default();

        let mut grants: HashMap<String, GrantRuntime> = HashMap::new();
        if let Ok(text) = std::fs::read_to_string(paths.grants()) {
            let defs: Vec<GrantDef> = serde_json::from_str(&text).context("grants.json corrupt")?;
            for def in defs {
                let name = def.name.clone();
                match def.compile() {
                    Ok(g) => {
                        grants.insert(name, g);
                    }
                    Err(e) => eprintln!("pherd: warning: grant '{name}' disabled: {e}"),
                }
            }
        }

        let bridges: HashMap<String, crate::bridge::BridgeDef> =
            std::fs::read_to_string(paths.bridges())
                .ok()
                .and_then(|t| serde_json::from_str::<Vec<crate::bridge::BridgeDef>>(&t).ok())
                .map(|v| v.into_iter().map(|b| (b.name.clone(), b)).collect())
                .unwrap_or_default();

        let semantic = crate::semantic::Semantic::new(paths.clone());
        let mut state = State {
            paths,
            node,
            matcher,
            metas,
            next_seq,
            timers,
            pending,
            tails: Vec::new(),
            listeners: HashMap::new(),
            cursors,
            grants,
            bridges,
            bridge_cancels: HashMap::new(),
            current_seq: None,
            retention_secs,
            semantic,
            judge_cfg: None,
            judge_tx: None,
            judge_budgets,
            verdict_cache,
            conditions,
            forwarded_seen,
            outbox,
            outbox_tx: None,
        };
        let has_judge_subs = state
            .matcher
            .ids()
            .iter()
            .any(|id| state.matcher.get(id).is_some_and(|s| s.judge.is_some()));
        if has_judge_subs {
            match crate::judge::resolve_config() {
                Ok(cfg) => state.judge_cfg = Some(cfg),
                Err(e) => eprintln!("pherd: warning: judge subscriptions cannot fire: {e}"),
            }
        }

        // Re-embed descriptors for meaning subscriptions that survived restart.
        let meaning_subs: Vec<String> = state.matcher.ids().iter().map(|s| s.to_string()).collect();
        for id in meaning_subs {
            let Some(sub) = state.matcher.get(&id) else {
                continue;
            };
            if let Some(meaning) = sub.meaning.clone() {
                if let Err(e) = state.semantic.on_register(&id, &meaning) {
                    eprintln!("pherd: warning: meaning subscription {id} cannot score: {e}");
                }
            }
        }
        state.persist_subs()?;
        state.gc()?;
        Ok(state)
    }

    fn persist_subs(&self) -> anyhow::Result<()> {
        let list: Vec<&SubMeta> = self.metas.values().collect();
        write_json_atomic(&self.paths.subs(), &serde_json::to_value(list)?)
    }

    fn persist_timers(&self) -> anyhow::Result<()> {
        write_json_atomic(&self.paths.timers(), &serde_json::to_value(&self.timers)?)
    }

    fn append_jsonl(&self, path: &std::path::Path, value: &Value) -> anyhow::Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let mut line = serde_json::to_string(value)?;
        line.push('\n');
        f.write_all(line.as_bytes())?;
        f.flush()?;
        Ok(())
    }

    // -- registration ------------------------------------------------------

    fn register(
        &mut self,
        string: &str,
        option_words: &[String],
        name: Option<String>,
    ) -> Result<Value, String> {
        let mut sub = Subscription::parse(string).map_err(|e| e.to_string())?;
        if !option_words.is_empty() {
            pher_core::subscription::apply_option_words(&mut sub, option_words)
                .map_err(|e| e.to_string())?;
        }
        // Honesty gate: `then stream` has nowhere to deliver without a live
        // listener connection — only `pher listen` / the SDK may register it.
        if sub.then.sink == Sink::Stream {
            return Err(
                "'then stream' requires a live listener connection — use `pher listen` \
                 or an SDK client, not `pher when`"
                    .to_string(),
            );
        }
        self.register_parsed(sub, name)
    }

    /// Registration path for `Listen` connections: the sink must be `stream`,
    /// and a durable lifetime is tightened to `while <client> alive` — which
    /// this path actually enforces via connection liveness.
    fn register_listener(
        &mut self,
        string: &str,
        option_words: &[String],
        client: &str,
    ) -> Result<Value, String> {
        let mut sub = Subscription::parse(string).map_err(|e| e.to_string())?;
        if !option_words.is_empty() {
            pher_core::subscription::apply_option_words(&mut sub, option_words)
                .map_err(|e| e.to_string())?;
        }
        if sub.then.sink != Sink::Stream {
            return Err(format!(
                "listen requires 'then stream', got 'then {}' — register push sinks with `pher when`",
                sub.then.sink.name()
            ));
        }
        if matches!(sub.lifetime, pher_core::Lifetime::Durable) {
            sub.lifetime = pher_core::Lifetime::Lease {
                lessee: client.to_string(),
            };
        }
        self.register_parsed(sub, None)
    }

    fn register_parsed(
        &mut self,
        sub: Subscription,
        name: Option<String>,
    ) -> Result<Value, String> {
        if let Some(n) = &name {
            if let Some(existing) = self.metas.values().find(|m| m.name.as_deref() == Some(n)) {
                return Err(format!(
                    "subscription name '{n}' already in use ({}) — `pher apply` reconciles, \
                     or rm it first",
                    existing.id
                ));
            }
        }
        // Honesty gate: a judge subscription needs a working provider config
        // NOW, not at first event — refuse registration with the exact fix.
        if sub.judge.is_some() {
            let cfg = crate::judge::resolve_config()
                .map_err(|e| format!("cannot enable judge tier: {e}"))?;
            self.judge_cfg = Some(cfg);
        }
        match sub.then.sink {
            Sink::Buz if sub.then.args.is_empty() => {
                return Err("'then buz' requires a bee name".to_string());
            }
            Sink::Http => {
                let method_ok = sub.then.args.first().is_some_and(|m| {
                    matches!(
                        m.to_uppercase().as_str(),
                        "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
                    )
                });
                let url_ok = sub
                    .then
                    .args
                    .get(1)
                    .is_some_and(|u| u.starts_with("http://") || u.starts_with("https://"));
                if !method_ok || !url_ok {
                    return Err(
                        "'then http' requires a method and url: http POST https://...".to_string(),
                    );
                }
            }
            Sink::Hermes | Sink::Pol | Sink::Hive if sub.then.args.is_empty() => {
                return Err(format!(
                    "'then {}' requires arguments",
                    sub.then.sink.name()
                ));
            }
            _ => {}
        }
        if sub.then.sink == Sink::Emit {
            match sub.then.args.first() {
                Some(subj) => {
                    SubjectPattern::parse(subj)
                        .ok()
                        .filter(|p| p.is_concrete())
                        .ok_or_else(|| {
                            format!("'then emit' requires a concrete subject, got '{subj}'")
                        })?;
                }
                None => return Err("'then emit' requires a subject".to_string()),
            }
        }

        let id = ids::short_id("PH");
        if let Some(meaning) = &sub.meaning {
            self.semantic
                .on_register(&id, meaning)
                .map_err(|e| format!("cannot enable meaning tier: {e}"))?;
        }
        let expires_at = match &sub.lifetime {
            pher_core::Lifetime::Ttl { ttl } => Some(now_unix() + ttl.secs()),
            _ => None,
        };
        let mut warnings: Vec<String> = Vec::new();
        if (sub.meaning.is_some() || sub.judge.is_some()) && sub.replay.is_some() {
            warnings.push(
                "replay ('since') does not evaluate the meaning/judge tiers; backlog \
                 events are matched on tiers 1-2 only"
                    .to_string(),
            );
        }
        // Stream subs are exempt: their lease IS enforced, by connection
        // liveness — disconnect removes the subscription.
        if matches!(sub.lifetime, pher_core::Lifetime::Lease { .. })
            && sub.then.sink != Sink::Stream
        {
            warnings.push(
                "lease liveness (while ... alive) is not heartbeat-checked yet; \
                 treat as durable until slice 3"
                    .to_string(),
            );
        }

        let meta = SubMeta {
            id: id.clone(),
            name,
            string: sub.canon(),
            json: sub.to_json(),
            created: now_ts(),
            expires_at,
            deliveries: 0,
        };
        self.metas.insert(id.clone(), meta);
        self.matcher.insert(id.clone(), sub.clone());
        self.persist_subs().map_err(|e| e.to_string())?;
        self.emit_bus_event("pher.subscription.registered", json!({ "id": id }));

        // Replay: run the backlog through the matcher first, then live.
        // Stream sinks replay in attach_listener instead — AFTER the listener
        // channel is attached, or every replayed delivery would be lost.
        let mut replayed = 0u64;
        if sub.then.sink != Sink::Stream {
            if let Some(replay) = sub.replay.clone() {
                let cutoff = unix_to_ts(now_unix().saturating_sub(replay.lookback.secs()));
                let backlog = self.read_events(None);
                for (seq, event) in backlog {
                    if event.ts >= cutoff {
                        self.current_seq = Some(seq);
                        if let Some(n) = self.try_deliver(&id, &event) {
                            replayed += n;
                        }
                    }
                }
            }
        }

        Ok(ok(json!({
            "id": id,
            "canonical": self.metas[&id].string,
            "replayedDeliveries": replayed,
            "warnings": warnings,
        })))
    }

    // -- ingest ------------------------------------------------------------

    pub(crate) fn ingest(&mut self, partial: PartialEvent, hops: u32) -> Result<Value, String> {
        let pattern = SubjectPattern::parse(&partial.subject).map_err(|e| e.to_string())?;
        if !pattern.is_concrete() {
            return Err(format!(
                "cannot emit on wildcard subject '{}'",
                partial.subject
            ));
        }
        let event = Envelope {
            id: ids::short_id("PH"),
            ts: now_ts(),
            node: self.node.clone(),
            source: partial.source.unwrap_or_else(|| "cli".to_string()),
            event_type: partial
                .event_type
                .unwrap_or_else(|| partial.subject.clone()),
            subject: partial.subject,
            correlation: partial.correlation,
            payload: partial.payload.unwrap_or(Value::Null),
            ttl_class: None,
            hops: (hops > 0).then_some(hops),
        };
        let (seq, deliveries) = self.ingest_envelope(event.clone())?;
        Ok(ok(json!({
            "id": event.id,
            "seq": seq,
            "deliveries": deliveries,
        })))
    }

    fn ingest_envelope(&mut self, event: Envelope) -> Result<(u64, u64), String> {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.current_seq = Some(seq);
        self.append_jsonl(&self.paths.events(), &json!({ "seq": seq, "event": event }))
            .map_err(|e| e.to_string())?;
        self.notify_tails(seq, &event);

        // Expect bookkeeping first: an incoming event can disarm timers.
        self.disarm_matching_timers(&event);

        let matched_ids: Vec<String> = self
            .matcher
            .match_ids(&event)
            .into_iter()
            .map(String::from)
            .collect();
        let mut deliveries = 0u64;
        for sub_id in matched_ids {
            let Some(sub) = self.matcher.get(&sub_id).cloned() else {
                continue;
            };
            if let Some(expect) = &sub.expect {
                // Origin match arms the absence timer; the action fires on expiry.
                self.timers.push(Timer {
                    sub_id: sub_id.clone(),
                    origin: event.clone(),
                    deadline: now_unix() + expect.within.secs(),
                    seq: Some(seq),
                });
                self.persist_timers().map_err(|e| e.to_string())?;
                continue;
            }
            if let Some(n) = self.try_deliver(&sub_id, &event) {
                deliveries += n;
            }
        }

        // Tiers 3-4: subscriptions whose tiers 1-2 passed but carry `meaning`
        // and/or `judge` clauses. The event is projected + embedded at most
        // ONCE, shared across all meaning candidates; judge survivors enqueue
        // for async verdicts (tier-4 matches deliver on verdict, not ingest).
        let pending_subs: Vec<String> = self
            .matcher
            .pending_ids(&event)
            .into_iter()
            .map(String::from)
            .collect();
        if !pending_subs.is_empty() {
            let needs_embedding = pending_subs
                .iter()
                .any(|id| self.matcher.get(id).is_some_and(|s| s.meaning.is_some()));
            let mut event_vec: Option<Vec<f32>> = None;
            if needs_embedding {
                match self.semantic.embed_event(&event) {
                    Ok(v) => event_vec = Some(v),
                    Err(e) => {
                        eprintln!("pherd: semantic tier unavailable for {}: {e}", event.id)
                    }
                }
            }
            let now = now_unix();
            for sub_id in pending_subs {
                let Some(sub) = self.matcher.get(&sub_id).cloned() else {
                    continue;
                };
                // Tier 3 gates tier 4.
                let mut meaning_record: Option<Value> = None;
                if let Some(meaning) = &sub.meaning {
                    let Some(vec) = &event_vec else { continue }; // embedder down: fail closed
                    let (passed, record) =
                        self.semantic.score(&sub_id, meaning, &event.id, vec, now);
                    if !passed {
                        continue;
                    }
                    meaning_record = Some(record);
                }
                if let Some(expect) = &sub.expect {
                    self.timers.push(Timer {
                        sub_id: sub_id.clone(),
                        origin: event.clone(),
                        deadline: now_unix() + expect.within.secs(),
                        seq: Some(seq),
                    });
                    self.persist_timers().map_err(|e| e.to_string())?;
                    continue;
                }
                if sub.judge.is_some() {
                    deliveries += self.judge_gate(&sub_id, &sub, &event, meaning_record);
                } else if let Some(record) = meaning_record {
                    deliveries += self.deliver_semantic(&sub_id, &sub, &event, record);
                }
            }
            if let Some(vec) = event_vec {
                self.semantic.record_event(now, &event.id, vec);
            }
        }
        Ok((seq, deliveries))
    }

    /// Tier 4 admission: cached verdicts apply instantly (replay never
    /// re-rolls); otherwise budget (fail-closed) then sample gate, then the
    /// job queues for the async judge worker.
    fn judge_gate(
        &mut self,
        sub_id: &str,
        sub: &Subscription,
        event: &Envelope,
        meaning_record: Option<Value>,
    ) -> u64 {
        let Some(judge_clause) = sub.judge.clone() else {
            return 0;
        };
        let summary = crate::judge::event_summary(event);
        let cache_key = format!("{sub_id}:{}", crate::judge::content_fingerprint(event));
        if let Some(verdict) = self.verdict_cache.get(&cache_key).cloned() {
            if verdict.verdict {
                let model = self
                    .judge_cfg
                    .as_ref()
                    .map(|c| c.model.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                return self.deliver_judged(
                    sub_id,
                    sub,
                    event,
                    meaning_record,
                    &verdict,
                    true,
                    &model,
                );
            }
            return 0;
        }
        if !self.reserve_judge_budget(sub_id, &judge_clause) {
            return 0; // budget exhausted: fail closed
        }
        if let Some(sample) = judge_clause.sample {
            if pseudo_random() > sample {
                return 0;
            }
        }
        let Some(cfg) = self.judge_cfg.clone() else {
            eprintln!("pherd: judge subscription {sub_id} has no provider config");
            return 0;
        };
        let Some(tx) = &self.judge_tx else {
            eprintln!("pherd: judge worker not running");
            return 0;
        };
        let _ = tx.send(JudgeJob {
            sub_id: sub_id.to_string(),
            event: event.clone(),
            seq: self.current_seq,
            summary,
            cache_key,
            question: judge_clause.question.clone(),
            meaning_record,
            config: cfg,
        });
        0 // delivery happens on verdict, asynchronously
    }

    /// Budget window accounting. Returns whether one judge call may proceed.
    /// The transition into exhaustion emits pher.subscription.budget_exhausted.
    fn reserve_judge_budget(&mut self, sub_id: &str, judge: &pher_core::Judge) -> bool {
        let now = now_unix();
        let period = crate::judge::period_secs(&judge.budget_period);
        let (allowed, notify) = {
            let b = self
                .judge_budgets
                .entry(sub_id.to_string())
                .or_insert(JudgeBudget {
                    period_start: now,
                    used: 0,
                    notified: false,
                });
            if now.saturating_sub(b.period_start) >= period {
                b.period_start = now;
                b.used = 0;
                b.notified = false;
            }
            if b.used < judge.budget_count {
                b.used += 1;
                (true, false)
            } else {
                let notify = !b.notified;
                b.notified = true;
                (false, notify)
            }
        };
        let _ = write_json_atomic(
            &self.paths.judge_budgets(),
            &serde_json::to_value(&self.judge_budgets).unwrap_or_default(),
        );
        if notify {
            self.emit_bus_event(
                "pher.subscription.budget_exhausted",
                json!({ "id": sub_id }),
            );
        }
        allowed
    }

    fn persist_conditions(&self) -> anyhow::Result<()> {
        write_json_atomic(
            &self.paths.conditions(),
            &serde_json::to_value(&self.conditions.defs)?,
        )?;
        self.persist_condition_state()
    }

    fn persist_condition_state(&self) -> anyhow::Result<()> {
        write_json_atomic(
            &self.paths.condition_state(),
            &serde_json::to_value(&self.conditions.states)?,
        )
    }

    /// Datapoint intake: evaluate conditions here at the edge; only the
    /// transitions become events. Returns what was emitted.
    pub(crate) fn ingest_metric(
        &mut self,
        metric: &str,
        value: f64,
        labels: &HashMap<String, String>,
    ) -> Vec<Value> {
        let transitions = self.conditions.ingest(metric, value, labels, now_unix());
        let mut emitted = Vec::new();
        for t in &transitions {
            let subject = if t.entered {
                "metric.condition.entered"
            } else {
                "metric.condition.cleared"
            };
            let payload = self.conditions.event_payload(t);
            let _ = self.ingest(
                PartialEvent {
                    subject: subject.to_string(),
                    payload: Some(payload),
                    event_type: None,
                    source: Some("tap.metrics".to_string()),
                    correlation: None,
                },
                0,
            );
            emitted.push(json!({ "condition": t.condition, "transition": subject }));
        }
        let _ = self.persist_condition_state();
        emitted
    }

    /// Ingest a delivery forwarded from another node (filter-at-source:
    /// the remote ran the cascade; only the match crossed the wire).
    /// Preserves the origin envelope (id/ts/node/correlation), dedups by
    /// the remote deliveryId, and honors the hop cycle guard.
    pub(crate) fn ingest_forwarded(&mut self, delivery: Value) -> Result<Value, String> {
        let event: Envelope = serde_json::from_value(
            delivery
                .get("event")
                .cloned()
                .ok_or("delivery must carry {event, ...}")?,
        )
        .map_err(|e| format!("invalid forwarded envelope: {e}"))?;
        self.admit_remote(event)
    }

    /// Admit an event that originated on another bus (push via /deliver, or
    /// pull via a bridge). Dedup is by EVENT id — the envelope's identity is
    /// preserved across any topology, so the same event arriving via two
    /// routes (or retried at-least-once) ingests exactly once. Persisted
    /// before ingest so restarts don't reopen the window.
    pub(crate) fn admit_remote(&mut self, mut event: Envelope) -> Result<Value, String> {
        if event.id.is_empty() {
            return Err("remote event has no id".to_string());
        }
        if self.forwarded_seen.contains(&event.id) {
            return Ok(json!({ "ok": true, "deduped": true }));
        }
        let _ = self.append_jsonl(&self.paths.forwarded(), &json!({ "id": event.id }));
        self.forwarded_seen.push_back(event.id.clone());
        while self.forwarded_seen.len() > MAX_FORWARDED_SEEN {
            self.forwarded_seen.pop_front();
        }
        let hops = event.hops.unwrap_or(0) + 1;
        if hops > MAX_EMIT_HOPS {
            return Err(format!(
                "hop cap ({MAX_EMIT_HOPS}) reached — forward cycle guard"
            ));
        }
        event.hops = Some(hops);
        let (seq, deliveries) = self.ingest_envelope(event.clone())?;
        Ok(json!({ "ok": true, "id": event.id, "seq": seq, "deliveries": deliveries }))
    }

    fn persist_grants(&self) -> anyhow::Result<()> {
        let defs: Vec<&GrantDef> = self.grants.values().map(|g| &g.def).collect();
        write_json_atomic(&self.paths.grants(), &serde_json::to_value(defs)?)?;
        // Tokens live in this file: owner-only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(
                self.paths.grants(),
                std::fs::Permissions::from_mode(0o600),
            );
        }
        Ok(())
    }

    fn persist_bridges(&self) -> anyhow::Result<()> {
        let defs: Vec<&crate::bridge::BridgeDef> = self.bridges.values().collect();
        write_json_atomic(&self.paths.bridges(), &serde_json::to_value(defs)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(
                self.paths.bridges(),
                std::fs::Permissions::from_mode(0o600),
            );
        }
        Ok(())
    }

    /// Resolve a bearer token to a grant name (http auth).
    pub(crate) fn grant_for_token(&self, token: &str) -> Option<String> {
        self.grants
            .values()
            .find(|g| g.def.token == token)
            .map(|g| g.def.name.clone())
    }

    /// May this grant emit this event? Checked against the `emit` filter
    /// with a probe envelope (grants should reference subject/payload).
    pub(crate) fn check_grant_emit(&self, grant: &str, event: &PartialEvent) -> Result<(), String> {
        let g = self
            .grants
            .get(grant)
            .ok_or_else(|| format!("unknown grant '{grant}'"))?;
        let Some(filter) = &g.emit_sub else {
            return Err(format!("grant '{grant}' has no emit filter"));
        };
        let probe = Envelope {
            id: String::new(),
            ts: String::new(),
            node: self.node.clone(),
            source: event
                .source
                .clone()
                .unwrap_or_else(|| format!("grant:{grant}")),
            event_type: event
                .event_type
                .clone()
                .unwrap_or_else(|| event.subject.clone()),
            subject: event.subject.clone(),
            correlation: event.correlation.clone(),
            payload: event.payload.clone().unwrap_or(Value::Null),
            ttl_class: None,
            hops: None,
        };
        if filter_matches(filter, &probe) {
            Ok(())
        } else {
            Err(format!(
                "grant '{grant}' may not emit '{}' (emit filter: {})",
                event.subject,
                g.def.emit.as_deref().unwrap_or("")
            ))
        }
    }

    /// Deliver a tier-4 match: extend the block with meaning + judge records.
    #[allow(clippy::too_many_arguments)]
    fn deliver_judged(
        &mut self,
        sub_id: &str,
        sub: &Subscription,
        event: &Envelope,
        meaning_record: Option<Value>,
        verdict: &crate::judge::Verdict,
        cached: bool,
        model: &str,
    ) -> u64 {
        if !verdict.verdict {
            return 0;
        }
        let eval = evaluate(sub_id, sub, event, None);
        let Some(mut block) = eval.match_block else {
            return 0;
        };
        if let Some(record) = meaning_record {
            block.meaning = Some(record);
            block.tiers.push("meaning".to_string());
        }
        block.judge = Some(json!({
            "verdict": verdict.verdict,
            "rationale": verdict.rationale,
            "model": model,
            "cached": cached,
        }));
        block.pending.clear();
        block.tiers.push("judge".to_string());
        self.shape_or_deliver(sub_id, sub, event, block)
    }

    /// Deliver a tier-3 match: the tiers 1-2 explanation block is extended
    /// with the meaning record, then shaped/delivered like any other match.
    fn deliver_semantic(
        &mut self,
        sub_id: &str,
        sub: &Subscription,
        event: &Envelope,
        meaning_record: Value,
    ) -> u64 {
        let eval = evaluate(sub_id, sub, event, None);
        let Some(mut block) = eval.match_block else {
            return 0;
        };
        block.meaning = Some(meaning_record);
        block.pending.clear();
        block.tiers.push("meaning".to_string());
        self.shape_or_deliver(sub_id, sub, event, block)
    }

    /// Run `event` through subscription `sub_id`'s cascade; on match, deliver
    /// immediately or queue into the subscription's shaping window.
    /// Returns Some(delivered-count) when the event matched (0 = queued).
    fn try_deliver(&mut self, sub_id: &str, event: &Envelope) -> Option<u64> {
        let sub = self.matcher.get(sub_id)?.clone();
        let eval = evaluate(sub_id, &sub, event, None);
        if !matches!(eval.outcome, Outcome::Matched) {
            return None;
        }
        let block = eval.match_block?;
        Some(self.shape_or_deliver(sub_id, &sub, event, block))
    }

    /// Deliver immediately or queue into the subscription's shaping window.
    /// Returns the delivered count (0 = queued for a later window close).
    fn shape_or_deliver(
        &mut self,
        sub_id: &str,
        sub: &Subscription,
        event: &Envelope,
        mut block: MatchBlock,
    ) -> u64 {
        match &sub.delivery {
            pher_core::Delivery::Immediate => {
                self.deliver(sub_id, sub, event, &mut block, None);
                1
            }
            pher_core::Delivery::Debounce { window } => {
                let seq = self.current_seq;
                if let Some(p) = self.pending.get_mut(sub_id) {
                    // Window open: collapse to the latest event, deliver at close.
                    p.events = vec![event.clone()];
                    p.seqs = seq.into_iter().collect();
                    p.collapsed += 1;
                    let _ = self.persist_pending();
                    0
                } else {
                    // Leading edge delivers immediately, then the window arms.
                    self.deliver(sub_id, sub, event, &mut block, None);
                    self.pending.insert(
                        sub_id.to_string(),
                        PendingWindow {
                            mode: "debounce".to_string(),
                            window_end: now_unix() + window.secs(),
                            events: Vec::new(),
                            seqs: Vec::new(),
                            collapsed: 0,
                        },
                    );
                    let _ = self.persist_pending();
                    1
                }
            }
            pher_core::Delivery::Batch { window } => {
                let seq = self.current_seq;
                let p = self
                    .pending
                    .entry(sub_id.to_string())
                    .or_insert_with(|| PendingWindow {
                        mode: "batch".to_string(),
                        window_end: now_unix() + window.secs(),
                        events: Vec::new(),
                        seqs: Vec::new(),
                        collapsed: 0,
                    });
                if p.events.len() >= MAX_BATCH_QUEUE {
                    p.events.remove(0);
                    if !p.seqs.is_empty() {
                        p.seqs.remove(0);
                    }
                    p.collapsed += 1;
                }
                p.events.push(event.clone());
                if let Some(s) = seq {
                    p.seqs.push(s);
                }
                let _ = self.persist_pending();
                0
            }
        }
    }

    fn persist_pending(&self) -> anyhow::Result<()> {
        write_json_atomic(&self.paths.pending(), &serde_json::to_value(&self.pending)?)
    }

    /// Close expired shaping windows: debounce delivers the trailing collapsed
    /// event (and re-arms), batch delivers the accumulated set once.
    fn fire_shaping_windows(&mut self) {
        let now = now_unix();
        let due: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, p)| p.window_end <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for sub_id in due {
            let Some(p) = self.pending.remove(&sub_id) else {
                continue;
            };
            let Some(sub) = self.matcher.get(&sub_id).cloned() else {
                let _ = self.persist_pending();
                continue; // subscription evaporated while the window was open
            };
            if p.events.is_empty() {
                // Idle debounce window: nothing suppressed, just disarm.
                let _ = self.persist_pending();
                continue;
            }
            let last = p.events.last().unwrap().clone();
            self.current_seq = p.seqs.last().copied();
            let eval = evaluate(&sub_id, &sub, &last, None);
            let Some(mut block) = eval.match_block else {
                let _ = self.persist_pending();
                continue;
            };
            let shaping = if p.mode == "batch" {
                json!({
                    "mode": "batch",
                    "count": p.events.len(),
                    "droppedOverCap": p.collapsed,
                    "events": p.events,
                })
            } else {
                json!({ "mode": "debounce", "collapsed": p.collapsed })
            };
            self.deliver(&sub_id, &sub, &last, &mut block, Some(shaping));
            if p.mode == "debounce" {
                // Re-arm so a burst can't exceed one delivery per window.
                if let pher_core::Delivery::Debounce { window } = &sub.delivery {
                    if self.metas.contains_key(&sub_id) {
                        self.pending.insert(
                            sub_id.clone(),
                            PendingWindow {
                                mode: "debounce".to_string(),
                                window_end: now_unix() + window.secs(),
                                events: Vec::new(),
                                seqs: Vec::new(),
                                collapsed: 0,
                            },
                        );
                    }
                }
            }
            let _ = self.persist_pending();
        }
    }

    fn deliver(
        &mut self,
        sub_id: &str,
        sub: &Subscription,
        event: &Envelope,
        block: &mut MatchBlock,
        shaping: Option<Value>,
    ) {
        let n = {
            let meta = match self.metas.get_mut(sub_id) {
                Some(m) => m,
                None => return,
            };
            meta.deliveries += 1;
            meta.deliveries
        };
        let delivery_id = format!("{sub_id}:{}:{n}", event.id);
        block.delivery_id = Some(delivery_id.clone());

        let sink_result = self.run_sink(sub, event, block, &delivery_id, shaping.as_ref());
        let mut record = json!({
            "deliveryId": delivery_id,
            "ts": now_ts(),
            "seq": self.current_seq,
            "event": event,
            "match": block,
            "sink": sink_result,
        });
        if let Some(shaping) = &shaping {
            record["shaping"] = shaping.clone();
        }
        let _ = self.append_jsonl(&self.paths.deliveries(), &record);

        // n-shot subscriptions retire after their last delivery.
        let retire = sub.limit.is_some_and(|limit| n >= limit);
        if retire {
            self.remove_sub(sub_id, "limit reached");
        } else {
            let _ = self.persist_subs();
        }
    }

    fn run_sink(
        &mut self,
        sub: &Subscription,
        event: &Envelope,
        block: &MatchBlock,
        delivery_id: &str,
        shaping: Option<&Value>,
    ) -> Value {
        let mut delivery_json = json!({ "event": event, "match": block });
        if let Some(seq) = self.current_seq {
            delivery_json["seq"] = json!(seq);
        }
        if let Some(shaping) = shaping {
            delivery_json["shaping"] = shaping.clone();
        }
        match sub.then.sink {
            Sink::Cmd => {
                let cmdline = sub.then.args.join(" ");
                let spawned = std::process::Command::new("sh")
                    .arg("-c")
                    .arg(&cmdline)
                    .env("PHER_DELIVERY", delivery_json.to_string())
                    .env(
                        "PHER_EVENT",
                        serde_json::to_string(event).unwrap_or_default(),
                    )
                    .env("PHER_SUBJECT", &event.subject)
                    .env("PHER_DELIVERY_ID", delivery_id)
                    .stdin(std::process::Stdio::null())
                    .spawn();
                match spawned {
                    Ok(child) => json!({ "sink": "cmd", "pid": child.id(), "cmd": cmdline }),
                    Err(e) => json!({ "sink": "cmd", "error": e.to_string() }),
                }
            }
            Sink::Emit => {
                let subject = sub.then.args.first().cloned().unwrap_or_default();
                let hops = event.hops.unwrap_or(0) + 1;
                if hops > MAX_EMIT_HOPS {
                    return json!({
                        "sink": "emit",
                        "error": format!("hop cap ({MAX_EMIT_HOPS}) reached — emit cycle guard")
                    });
                }
                // The re-emitted event carries the match block in its payload
                // metadata and preserves correlation, so trails stay intact.
                let re = PartialEvent {
                    subject: subject.clone(),
                    payload: Some(json!({
                        "event": event.payload,
                        "via": { "match": block, "originalSubject": event.subject },
                    })),
                    event_type: Some(subject.clone()),
                    source: Some(format!("pher.emit/{}", block.subscription)),
                    correlation: event.correlation.clone(),
                };
                match self.ingest(re, hops) {
                    Ok(v) => json!({ "sink": "emit", "subject": subject, "result": v }),
                    Err(e) => json!({ "sink": "emit", "error": e }),
                }
            }
            Sink::Buz => {
                let bee = sub.then.args[0].clone();
                let summary = format!(
                    "[pheromone {delivery_id}] {} — payload: {}",
                    event.subject,
                    truncate(&event.payload.to_string(), 500)
                );
                let spawned = std::process::Command::new("hive")
                    .args(["buz", "send", &bee, "--sender", "pher", "-p", &summary])
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .spawn();
                match spawned {
                    Ok(child) => json!({ "sink": "buz", "bee": bee, "pid": child.id() }),
                    Err(e) => json!({ "sink": "buz", "bee": bee, "error": e.to_string() }),
                }
            }
            Sink::Http => {
                // `then http POST <url>` — delivery JSON as the request body,
                // via the persistent outbox: the worker attempts immediately
                // and retries with backoff until the receiver accepts or the
                // entry outlives retention. A dead hub means delayed, not
                // lost (the receiving side dedups by deliveryId).
                let method = sub.then.args[0].to_uppercase();
                let url = sub.then.args[1].clone();
                self.outbox.push(OutboxEntry {
                    delivery_id: delivery_id.to_string(),
                    method: method.clone(),
                    url: url.clone(),
                    body: delivery_json.to_string(),
                    attempts: 0,
                    next_try: 0,
                    created: now_unix(),
                });
                let _ = self.persist_outbox();
                if let Some(tx) = &self.outbox_tx {
                    let _ = tx.send(());
                }
                json!({ "sink": "http", "method": method, "url": url, "queued": true })
            }
            Sink::Hermes => {
                // `then hermes <invoke...>` — hand off to the Hermes CLI.
                spawn_cli_sink("hermes", &sub.then.args, &delivery_json, delivery_id)
            }
            Sink::Pol => {
                // `then pol fire <trigger>` — hand off to the Pollinate CLI.
                spawn_cli_sink("pol", &sub.then.args, &delivery_json, delivery_id)
            }
            Sink::Hive => {
                // `then hive spawn|send|flow ...` — hand off to the hive CLI.
                spawn_cli_sink("hive", &sub.then.args, &delivery_json, delivery_id)
            }
            Sink::Stream => {
                // Delivery goes down the socket of whoever registered this
                // subscription. No listener means the sub is mid-teardown.
                delivery_json["deliveryId"] = json!(delivery_id);
                match self.listeners.get(&block.subscription) {
                    // Grant enforcement: the delivery must also match the
                    // listener's allow filter — intersection, not trust.
                    Some(handle)
                        if handle
                            .filter
                            .as_ref()
                            .is_some_and(|f| !filter_matches(f, event)) =>
                    {
                        json!({ "sink": "stream", "filtered": "grant allow" })
                    }
                    Some(handle) if handle.tx.send(delivery_json.to_string()).is_ok() => {
                        json!({ "sink": "stream", "delivered": true })
                    }
                    _ => json!({ "sink": "stream", "error": "listener gone" }),
                }
            }
        }
    }

    /// The bus eats its own dog food: lifecycle events are ordinary events.
    fn emit_bus_event(&mut self, subject: &str, payload: Value) {
        let _ = self.ingest(
            PartialEvent {
                subject: subject.to_string(),
                payload: Some(payload),
                event_type: None,
                source: Some("pherd".to_string()),
                correlation: None,
            },
            0,
        );
    }

    fn remove_sub(&mut self, id: &str, reason: &str) -> bool {
        let removed = self.matcher.remove(id);
        if removed {
            self.metas.remove(id);
            self.timers.retain(|t| t.sub_id != id);
            self.pending.remove(id);
            // Dropping the sender ends the listener's delivery loop, which
            // closes its connection.
            self.listeners.remove(id);
            self.semantic.on_remove(id);
            self.judge_budgets.remove(id);
            let _ = self.persist_subs();
            let _ = self.persist_timers();
            let _ = self.persist_pending();
            self.emit_bus_event(
                "pher.subscription.evaporated",
                json!({ "id": id, "reason": reason }),
            );
        }
        removed
    }

    // -- expect timers -----------------------------------------------------

    fn disarm_matching_timers(&mut self, event: &Envelope) {
        let mut disarmed = false;
        let matcher = &self.matcher;
        self.timers.retain(|timer| {
            let Some(sub) = matcher.get(&timer.sub_id) else {
                disarmed = true;
                return false;
            };
            let Some(expect) = &sub.expect else {
                disarmed = true;
                return false;
            };
            if !expect.subject.matches(&event.subject) {
                return true;
            }
            let joined = match &expect.where_expr {
                None => true,
                Some(w) => pher_core::expr::eval_bool(
                    w,
                    &pher_core::EvalCtx::new(event, Some(&timer.origin)),
                )
                .unwrap_or(false),
            };
            if joined {
                disarmed = true;
                false
            } else {
                true
            }
        });
        if disarmed {
            let _ = self.persist_timers();
        }
    }

    fn fire_expired_timers(&mut self) {
        let now = now_unix();
        let expired: Vec<Timer> = {
            let (expired, alive): (Vec<Timer>, Vec<Timer>) =
                self.timers.drain(..).partition(|t| t.deadline <= now);
            self.timers = alive;
            expired
        };
        if expired.is_empty() {
            return;
        }
        let _ = self.persist_timers();
        for timer in expired {
            let Some(sub) = self.matcher.get(&timer.sub_id).cloned() else {
                continue;
            };
            // Cursor bookkeeping: an absence delivery carries its origin's
            // seq. Consumers track max(seq), so this never regresses them.
            self.current_seq = timer.seq;
            let expect = match &sub.expect {
                Some(e) => e,
                None => continue,
            };
            let mut block = MatchBlock {
                subscription: timer.sub_id.clone(),
                tiers: vec!["expect".to_string()],
                where_rec: None,
                meaning: None,
                judge: None,
                pending: Vec::new(),
                delivery_id: None,
            };
            // The delivery payload is the origin event: "this happened and the
            // expected follow-up did not arrive within the window".
            let _ = expect;
            self.deliver(&timer.sub_id.clone(), &sub, &timer.origin, &mut block, None);
        }
    }

    fn sweep_expired_subs(&mut self) {
        let now = now_unix();
        let expired: Vec<String> = self
            .metas
            .values()
            .filter(|m| m.expires_at.is_some_and(|t| t <= now))
            .map(|m| m.id.clone())
            .collect();
        for id in expired {
            self.remove_sub(&id, "ttl expired");
        }
    }

    fn persist_outbox(&self) -> anyhow::Result<()> {
        write_json_atomic(&self.paths.outbox(), &serde_json::to_value(&self.outbox)?)
    }

    // -- consumer cursors ----------------------------------------------------

    fn persist_cursors(&self) -> anyhow::Result<()> {
        write_json_atomic(&self.paths.cursors(), &serde_json::to_value(&self.cursors)?)
    }

    /// Monotonic commit: a cursor never moves backward. Returns the stored seq.
    fn commit_cursor(&mut self, name: &str, seq: u64) -> u64 {
        let entry = self
            .cursors
            .entry(name.to_string())
            .or_insert(CursorEntry { seq: 0, ts: 0 });
        entry.seq = entry.seq.max(seq);
        entry.ts = now_unix();
        let stored = entry.seq;
        let _ = self.persist_cursors();
        stored
    }

    // -- events / tail / introspection -------------------------------------

    fn read_events(&self, after: Option<u64>) -> Vec<(u64, Envelope)> {
        read_events_file(&self.paths, after)
    }

    fn notify_tails(&mut self, seq: u64, event: &Envelope) {
        let line = json!({ "seq": seq, "event": event }).to_string();
        self.tails.retain(|client| {
            if let Some(pat) = &client.subject {
                if !pat.matches(&event.subject) {
                    return true; // filtered out, but keep the client
                }
            }
            client.tx.send(line.clone()).is_ok()
        });
    }

    fn gc(&mut self) -> anyhow::Result<()> {
        let cutoff = unix_to_ts(now_unix().saturating_sub(self.retention_secs));
        gc_jsonl(&self.paths.events(), |v| {
            v.get("event")
                .and_then(|e| e.get("ts"))
                .and_then(|t| t.as_str())
                .is_none_or(|ts| ts >= cutoff.as_str())
        })?;
        gc_jsonl(&self.paths.deliveries(), |v| {
            v.get("ts")
                .and_then(|t| t.as_str())
                .is_none_or(|ts| ts >= cutoff.as_str())
        })?;
        self.semantic
            .gc(now_unix().saturating_sub(self.retention_secs));
        let cutoff_unix = now_unix().saturating_sub(self.retention_secs);
        gc_jsonl(&self.paths.verdicts(), |v| {
            v.get("ts")
                .and_then(|t| t.as_u64())
                .is_none_or(|ts| ts >= cutoff_unix)
        })?;
        // Cursors evaporate with the events they point into: one not
        // committed for a full retention window points at nothing.
        let before = self.cursors.len();
        self.cursors.retain(|_, e| e.ts >= cutoff_unix);
        if self.cursors.len() != before {
            let _ = self.persist_cursors();
        }
        // Compact the forwarding dedup log to the in-memory window.
        if self.forwarded_seen.len() >= MAX_FORWARDED_SEEN {
            let keep: std::collections::HashSet<&String> = self.forwarded_seen.iter().collect();
            gc_jsonl(&self.paths.forwarded(), |v| {
                v.get("id")
                    .and_then(|i| i.as_str())
                    .is_none_or(|id| keep.contains(&id.to_string()))
            })?;
        }
        Ok(())
    }
}

/// Spawn a CLI sink (`hermes`, `pol`, `hive`) with the delivery in env.
/// Best-effort: spawn failures are recorded, exit codes are not awaited.
fn spawn_cli_sink(bin: &str, args: &[String], delivery_json: &Value, delivery_id: &str) -> Value {
    let spawned = std::process::Command::new(bin)
        .args(args)
        .env("PHER_DELIVERY", delivery_json.to_string())
        .env("PHER_DELIVERY_ID", delivery_id)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    match spawned {
        Ok(child) => json!({ "sink": bin, "pid": child.id(), "args": args }),
        Err(e) => json!({ "sink": bin, "error": e.to_string() }),
    }
}

/// Cheap uniform-ish [0,1) for `sample` gating; not security-sensitive.
fn pseudo_random() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 10_000) / 10_000.0
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &s[..end])
    }
}

pub(crate) fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown-node".to_string())
}

fn last_seq(paths: &Paths) -> Option<u64> {
    let text = std::fs::read_to_string(paths.events()).ok()?;
    text.lines()
        .rev()
        .find_map(|l| serde_json::from_str::<Value>(l).ok())
        .and_then(|v| v.get("seq").and_then(|s| s.as_u64()))
}

fn read_events_file(paths: &Paths, after: Option<u64>) -> Vec<(u64, Envelope)> {
    let Ok(text) = std::fs::read_to_string(paths.events()) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| {
            let seq = v.get("seq")?.as_u64()?;
            if after.is_some_and(|a| seq <= a) {
                return None;
            }
            let event: Envelope = serde_json::from_value(v.get("event")?.clone()).ok()?;
            Some((seq, event))
        })
        .collect()
}

/// Rewrite a JSONL file keeping only lines that pass `keep`. Evaporation.
fn gc_jsonl(path: &std::path::Path, keep: impl Fn(&Value) -> bool) -> anyhow::Result<()> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Ok(());
    };
    let kept: Vec<&str> = text
        .lines()
        .filter(|l| {
            serde_json::from_str::<Value>(l)
                .map(|v| keep(&v))
                .unwrap_or(false)
        })
        .collect();
    if kept.len() != text.lines().count() {
        let tmp = path.with_extension("tmp");
        let mut body = kept.join("\n");
        if !body.is_empty() {
            body.push('\n');
        }
        std::fs::write(&tmp, body)?;
        std::fs::rename(&tmp, path)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Server loop
// ---------------------------------------------------------------------------

pub fn run(paths: Paths) -> anyhow::Result<()> {
    // Fail fast on ingress misconfiguration (e.g. public bind without token)
    // before any socket exists or lifecycle events fire.
    let http_cfg = crate::http::resolve_config()?;

    let sock_path = paths.sock();
    if sock_path.exists() {
        // Stale socket from a crashed daemon, or a live one. Try connecting.
        if UnixStream::connect(&sock_path).is_ok() {
            bail!("pherd is already running on {}", sock_path.display());
        }
        std::fs::remove_file(&sock_path)?;
    }
    let state = Arc::new(Mutex::new(State::load(paths.clone())?));
    let listener = UnixListener::bind(&sock_path)
        .with_context(|| format!("cannot bind {}", sock_path.display()))?;

    {
        let mut s = state.lock().unwrap();
        eprintln!(
            "pherd listening on {} — node '{}', {} subscription(s), next seq {}",
            sock_path.display(),
            s.node,
            s.matcher.len(),
            s.next_seq
        );
        let node = s.node.clone();
        s.emit_bus_event("pher.node.online", json!({ "node": node }));
    }

    // Judge worker: verdicts happen off the state lock; delivery re-acquires.
    {
        let (jtx, jrx) = mpsc::channel::<JudgeJob>();
        state.lock().unwrap().judge_tx = Some(jtx);
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            for job in jrx {
                let result = crate::judge::ask(&job.config, &job.question, &job.summary);
                let mut s = state.lock().unwrap();
                match result {
                    Ok(verdict) => {
                        s.verdict_cache
                            .insert(job.cache_key.clone(), verdict.clone());
                        let record = json!({
                            "ts": now_unix(),
                            "key": job.cache_key,
                            "subscription": job.sub_id,
                            "event": job.event.id,
                            "model": job.config.model,
                            "verdict": verdict.verdict,
                            "rationale": verdict.rationale,
                        });
                        let verdicts_path = s.paths.verdicts();
                        let _ = s.append_jsonl(&verdicts_path, &record);
                        if verdict.verdict {
                            if let Some(sub) = s.matcher.get(&job.sub_id).cloned() {
                                s.current_seq = job.seq;
                                s.deliver_judged(
                                    &job.sub_id,
                                    &sub,
                                    &job.event,
                                    job.meaning_record,
                                    &verdict,
                                    false,
                                    &job.config.model,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("pherd: judge call failed for {}: {e}", job.sub_id);
                        s.emit_bus_event(
                            "pher.judge.error",
                            json!({ "subscription": job.sub_id, "event": job.event.id, "error": e }),
                        );
                    }
                }
            }
        });
    }

    // Outbox worker: outbound HTTP deliveries with retry. Attempts happen
    // off the state lock; a nudge after enqueue keeps the happy path fast,
    // the 1s tick catches scheduled retries.
    {
        let (otx, orx) = mpsc::channel::<()>();
        state.lock().unwrap().outbox_tx = Some(otx);
        let state = Arc::clone(&state);
        std::thread::spawn(move || loop {
            let _ = orx.recv_timeout(Duration::from_secs(1));
            let now = now_unix();
            let due: Vec<OutboxEntry> = {
                let s = state.lock().unwrap();
                s.outbox
                    .iter()
                    .filter(|e| e.next_try <= now)
                    .take(32)
                    .cloned()
                    .collect()
            };
            if due.is_empty() {
                continue;
            }
            let sink_token = std::env::var("PHER_HTTP_SINK_TOKEN").ok();
            for entry in due {
                let mut req = ureq::request(&entry.method, &entry.url)
                    .timeout(Duration::from_secs(10))
                    .set("content-type", "application/json");
                if let Some(token) = &sink_token {
                    req = req.set("authorization", &format!("Bearer {token}"));
                }
                let result = req.send_string(&entry.body);
                let mut s = state.lock().unwrap();
                let Some(pos) = s
                    .outbox
                    .iter()
                    .position(|e| e.delivery_id == entry.delivery_id && e.url == entry.url)
                else {
                    continue; // already resolved elsewhere
                };
                match result {
                    Ok(_) => {
                        s.outbox.remove(pos);
                    }
                    Err(e) => {
                        let error = truncate(&e.to_string(), 300);
                        let retention = s.retention_secs;
                        let expired = now_unix().saturating_sub(entry.created) > retention;
                        if expired {
                            // Evaporation bound: give up loudly, once.
                            s.outbox.remove(pos);
                            s.emit_bus_event(
                                "pher.delivery.failed",
                                json!({
                                    "deliveryId": entry.delivery_id,
                                    "sink": "http",
                                    "url": entry.url,
                                    "attempts": entry.attempts + 1,
                                    "error": error,
                                }),
                            );
                        } else {
                            let e = &mut s.outbox[pos];
                            e.attempts += 1;
                            e.next_try = now_unix() + outbox_backoff(e.attempts);
                            let first_failure = e.attempts == 1;
                            let attempts = e.attempts;
                            if first_failure {
                                // Transition event, not a per-retry firehose.
                                s.emit_bus_event(
                                    "pher.delivery.retrying",
                                    json!({
                                        "deliveryId": entry.delivery_id,
                                        "sink": "http",
                                        "url": entry.url,
                                        "error": error,
                                    }),
                                );
                            } else if attempts % 10 == 0 {
                                eprintln!(
                                    "pherd: outbox {} still failing after {} attempts: {}",
                                    entry.delivery_id, attempts, error
                                );
                            }
                        }
                    }
                }
                let _ = s.persist_outbox();
            }
        });
    }

    // Bridge workers for persisted bridges.
    {
        let defs: Vec<crate::bridge::BridgeDef> =
            state.lock().unwrap().bridges.values().cloned().collect();
        for def in defs {
            let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
            state
                .lock()
                .unwrap()
                .bridge_cancels
                .insert(def.name.clone(), Arc::clone(&cancel));
            crate::bridge::spawn(Arc::clone(&state), def, cancel);
        }
    }

    // HTTP ingress (webhooks, remote emit, metric intake) if configured.
    if let Some(http_cfg) = http_cfg {
        crate::http::start(Arc::clone(&state), http_cfg)?;
    }

    // Housekeeping: expect-timer expiry every second, GC sweep every 10 minutes.
    {
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            let mut last_gc = now_unix();
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let mut s = state.lock().unwrap();
                s.fire_expired_timers();
                s.fire_shaping_windows();
                s.sweep_expired_subs();
                if now_unix() - last_gc >= 600 {
                    let _ = s.gc();
                    last_gc = now_unix();
                }
            }
        });
    }

    for conn in listener.incoming() {
        let Ok(stream) = conn else { continue };
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            let _ = handle_connection(stream, state);
        });
    }
    Ok(())
}

/// Connections are persistent: one JSON-line request per line, one response
/// line each, until EOF. `tail` switches the connection into streaming mode
/// and owns it until the client goes away (taps hold an emit connection open
/// for their whole life).
fn handle_connection(stream: UnixStream, state: Arc<Mutex<State>>) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Ok(());
        }
        if line.trim().is_empty() {
            continue;
        }
        let request: Request = match serde_json::from_str(line.trim()) {
            Ok(r) => r,
            Err(e) => {
                respond(&stream, &err(format!("bad request: {e}")))?;
                continue;
            }
        };
        if let Request::Tail { after, subject } = request {
            handle_tail(after, subject, &stream, &state)?;
            return Ok(()); // tail owns the connection until the client leaves
        }
        if let Request::Listen {
            string,
            options,
            client,
            after,
            cursor,
        } = request
        {
            handle_listen(&string, &options, client, after, cursor, &stream, &state)?;
            return Ok(()); // listen owns the connection; disconnect = teardown
        }
        let response = handle_rpc(request, &state);
        respond(&stream, &response)?;
    }
}

/// Every non-streaming protocol op, shared by the unix socket and HTTP /rpc.
pub(crate) fn handle_rpc(request: Request, state: &Arc<Mutex<State>>) -> Value {
    match request {
        Request::Emit { event } => state.lock().unwrap().ingest(event, 0).unwrap_or_else(err),
        Request::When {
            string,
            options,
            name,
        } => state
            .lock()
            .unwrap()
            .register(&string, &options, name)
            .unwrap_or_else(err),
        Request::Ls => {
            let s = state.lock().unwrap();
            let mut subs: Vec<&SubMeta> = s.metas.values().collect();
            subs.sort_by(|a, b| a.created.cmp(&b.created));
            ok(json!({ "subs": subs }))
        }
        Request::Rm { id } => {
            let removed = state.lock().unwrap().remove_sub(&id, "removed by operator");
            ok(json!({ "removed": removed }))
        }
        Request::Status => {
            let s = state.lock().unwrap();
            ok(json!({
                "node": s.node,
                "subscriptions": s.matcher.len(),
                "nextSeq": s.next_seq,
                "armedTimers": s.timers.len(),
                "tails": s.tails.len(),
                "listeners": s.listeners.len(),
                "cursors": s.cursors.len(),
                "outbox": s.outbox.len(),
                "bridges": s.bridges.len(),
                "grants": s.grants.len(),
                "retention": format!("{}s", s.retention_secs),
                "semantic": s.semantic.status(),
                "conditions": s.conditions.defs.len(),
                "judge": match &s.judge_cfg {
                    Some(c) => json!({
                        "configured": true,
                        "provider": c.provider,
                        "model": c.model,
                        "cachedVerdicts": s.verdict_cache.len(),
                    }),
                    None => json!({ "configured": false, "cachedVerdicts": s.verdict_cache.len() }),
                },
                "home": s.paths.home.display().to_string(),
            }))
        }
        Request::ConditionAdd { def } => {
            let result = serde_json::from_value::<crate::conditions::ConditionDef>(def)
                .map_err(|e| format!("invalid condition: {e}"))
                .and_then(|d| {
                    let mut s = state.lock().unwrap();
                    s.conditions.add(d)?;
                    s.persist_conditions().map_err(|e| e.to_string())?;
                    Ok(())
                });
            match result {
                Ok(()) => ok(json!({})),
                Err(e) => err(e),
            }
        }
        Request::ConditionLs => {
            let s = state.lock().unwrap();
            ok(json!({ "conditions": s.conditions.status() }))
        }
        Request::ConditionRm { name } => {
            let mut s = state.lock().unwrap();
            let removed = s.conditions.remove(&name);
            let _ = s.persist_conditions();
            ok(json!({ "removed": removed }))
        }
        Request::Why { delivery_id } => {
            let s = state.lock().unwrap();
            let text = std::fs::read_to_string(s.paths.deliveries()).unwrap_or_default();
            drop(s);
            let record = text
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .find(|v| v.get("deliveryId").and_then(|d| d.as_str()) == Some(&delivery_id));
            match record {
                Some(r) => ok(json!({ "delivery": r })),
                None => err(format!("no delivery '{delivery_id}'")),
            }
        }
        Request::WhyNot { sub, event } => rpc_why_not(&sub, &event, state),
        Request::Tail { .. } => err("tail is a streaming op; use the unix socket"),
        Request::Listen { .. } => {
            err("listen is a streaming op; use the unix socket or POST /listen")
        }
        Request::CursorCommit { name, seq } => {
            let stored = state.lock().unwrap().commit_cursor(&name, seq);
            ok(json!({ "name": name, "seq": stored }))
        }
        Request::CursorLs => {
            let s = state.lock().unwrap();
            let mut cursors: Vec<Value> = s
                .cursors
                .iter()
                .map(|(name, e)| json!({ "name": name, "seq": e.seq, "committedAt": unix_to_ts(e.ts) }))
                .collect();
            cursors.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
            ok(json!({ "cursors": cursors, "head": s.next_seq.saturating_sub(1) }))
        }
        Request::CursorRm { name } => {
            let mut s = state.lock().unwrap();
            let removed = s.cursors.remove(&name).is_some();
            let _ = s.persist_cursors();
            ok(json!({ "removed": removed }))
        }
        Request::BridgeAdd { def } => {
            let mut def: crate::bridge::BridgeDef = match serde_json::from_value(def) {
                Ok(d) => d,
                Err(e) => return err(format!("invalid bridge: {e}")),
            };
            if let Err(e) = Subscription::parse(&def.sub_text()) {
                return err(format!("bridge '{}': bad sub: {e}", def.name));
            }
            let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
            {
                let mut s = state.lock().unwrap();
                if def.cursor.is_empty() {
                    // Scoped to this consuming bus, so two team members
                    // bridging the same upstream never share a position.
                    def.cursor = format!("bridge:{}@{}", def.name, s.node);
                }
                // Re-add = replace: the old worker winds down on its next
                // heartbeat/line; its upstream sub evaporates on disconnect.
                if let Some(old) = s.bridge_cancels.remove(&def.name) {
                    old.store(true, std::sync::atomic::Ordering::Relaxed);
                }
                s.bridges.insert(def.name.clone(), def.clone());
                if let Err(e) = s.persist_bridges() {
                    return err(format!("cannot persist bridge: {e}"));
                }
                s.bridge_cancels
                    .insert(def.name.clone(), Arc::clone(&cancel));
            }
            crate::bridge::spawn(Arc::clone(state), def.clone(), cancel);
            ok(json!({ "name": def.name, "cursor": def.cursor }))
        }
        Request::BridgeLs => {
            let s = state.lock().unwrap();
            let mut bridges: Vec<Value> = s
                .bridges
                .values()
                .map(|b| {
                    json!({
                        "name": b.name,
                        "url": b.url,
                        "sub": b.sub,
                        "cursor": b.cursor,
                        "authed": b.token.is_some(),
                    })
                })
                .collect();
            bridges.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
            ok(json!({ "bridges": bridges }))
        }
        Request::BridgeRm { name } => {
            let mut s = state.lock().unwrap();
            if let Some(cancel) = s.bridge_cancels.remove(&name) {
                cancel.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            let removed = s.bridges.remove(&name).is_some();
            let _ = s.persist_bridges();
            ok(json!({ "removed": removed }))
        }
        Request::GrantSet { def } => {
            let def: GrantDef = match serde_json::from_value(def) {
                Ok(d) => d,
                Err(e) => return err(format!("invalid grant: {e}")),
            };
            if def.token.len() < 16 {
                return err("grant tokens must be at least 16 characters");
            }
            let name = def.name.clone();
            let fingerprint = def.fingerprint();
            match def.compile() {
                Ok(runtime) => {
                    let mut s = state.lock().unwrap();
                    s.grants.insert(name.clone(), runtime);
                    if let Err(e) = s.persist_grants() {
                        return err(format!("cannot persist grant: {e}"));
                    }
                    ok(json!({ "name": name, "tokenFingerprint": fingerprint }))
                }
                Err(e) => err(e),
            }
        }
        Request::GrantLs => {
            let s = state.lock().unwrap();
            let mut grants: Vec<Value> = s
                .grants
                .values()
                .map(|g| {
                    json!({
                        "name": g.def.name,
                        "allow": g.def.allow,
                        "emit": g.def.emit,
                        "tokenFingerprint": g.def.fingerprint(),
                    })
                })
                .collect();
            grants.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
            ok(json!({ "grants": grants }))
        }
        Request::GrantRm { name } => {
            let mut s = state.lock().unwrap();
            let removed = s.grants.remove(&name).is_some();
            let _ = s.persist_grants();
            ok(json!({ "removed": removed }))
        }
    }
}

fn rpc_why_not(sub: &str, event: &str, state: &Arc<Mutex<State>>) -> Value {
    let s = state.lock().unwrap();
    let Some(subscription) = s.matcher.get(sub).cloned() else {
        return err(format!("no subscription '{sub}'"));
    };
    let found = s
        .read_events(None)
        .into_iter()
        .map(|(_, e)| e)
        .find(|e| e.id == event);
    drop(s);
    let Some(envelope) = found else {
        return err(format!("no event '{event}' in the log"));
    };
    let mut report = why_not(sub, &subscription, &envelope);
    // Tier 3/4 diagnosis: if tiers 1-2 passed, actually score the meaning
    // clause, and report the judge's cached verdict (why-not never spends
    // judge budget).
    let pending_semantic = report["matched"] == json!(false)
        && report["rejectedAt"] == json!(null)
        && (subscription.meaning.is_some() || subscription.judge.is_some());
    if pending_semantic {
        let mut s = state.lock().unwrap();
        let mut meaning_result: Option<(bool, Value)> = None;
        if let Some(meaning) = subscription.meaning.clone() {
            match s.semantic.embed_event(&envelope) {
                Ok(vec) => {
                    meaning_result =
                        Some(
                            s.semantic
                                .score(sub, &meaning, &envelope.id, &vec, now_unix()),
                        );
                }
                Err(e) => {
                    report["reason"] = json!(format!("meaning tier unavailable: {e}"));
                }
            }
        }
        let meaning_passed = meaning_result.as_ref().map(|(p, _)| *p);
        let meaning_record = meaning_result.map(|(_, r)| r);
        if meaning_passed == Some(false) {
            report = json!({
                "subscription": sub,
                "event": envelope.id,
                "matched": false,
                "rejectedAt": "meaning",
                "meaning": meaning_record,
            });
        } else if subscription.judge.is_some() {
            let key = format!("{sub}:{}", crate::judge::content_fingerprint(&envelope));
            let cached = s.verdict_cache.get(&key).cloned();
            let model = s
                .judge_cfg
                .as_ref()
                .map(|c| c.model.clone())
                .unwrap_or_else(|| "unconfigured".to_string());
            report = match cached {
                Some(v) => json!({
                    "subscription": sub,
                    "event": envelope.id,
                    "matched": v.verdict,
                    "rejectedAt": if v.verdict { Value::Null } else { json!("judge") },
                    "meaning": meaning_record,
                    "judge": {
                        "verdict": v.verdict,
                        "rationale": v.rationale,
                        "model": model,
                        "cached": true,
                    },
                }),
                None => json!({
                    "subscription": sub,
                    "event": envelope.id,
                    "matched": false,
                    "rejectedAt": null,
                    "meaning": meaning_record,
                    "reason": "no cached judge verdict; why-not never spends budget — emit a matching event to consult the judge",
                }),
            };
        } else if let Some(record) = meaning_record {
            report = json!({
                "subscription": sub,
                "event": envelope.id,
                "matched": true,
                "rejectedAt": null,
                "meaning": record,
            });
        }
    }
    ok(json!({ "report": report }))
}

/// Attach a raw-event tail: register the live feed and snapshot the backlog
/// atomically (seq dedup at the handoff makes the seam exact). Shared by the
/// unix socket and HTTP /tail. `last` truncates the backlog to its tail —
/// consoles want recent context, not a week of history.
pub(crate) fn attach_tail(
    state: &Arc<Mutex<State>>,
    after: Option<u64>,
    subject: Option<String>,
    last: Option<usize>,
) -> Result<
    (
        Vec<(u64, Envelope)>,
        mpsc::Receiver<String>,
        Option<SubjectPattern>,
    ),
    Value,
> {
    let pattern = match subject.map(|s| SubjectPattern::parse(&s)).transpose() {
        Ok(p) => p,
        Err(e) => return Err(err(e)),
    };
    let (tx, rx) = mpsc::channel::<String>();
    let mut s = state.lock().unwrap();
    s.tails.push(TailClient {
        tx,
        subject: pattern.clone(),
    });
    let mut backlog = s.read_events(after);
    if let Some(p) = &pattern {
        backlog.retain(|(_, e)| p.matches(&e.subject));
    }
    if let Some(n) = last {
        if backlog.len() > n {
            backlog.drain(..backlog.len() - n);
        }
    }
    Ok((backlog, rx, pattern))
}

fn handle_tail(
    after: Option<u64>,
    subject: Option<String>,
    stream: &UnixStream,
    state: &Arc<Mutex<State>>,
) -> anyhow::Result<()> {
    let (backlog, rx, _pattern) = match attach_tail(state, after, subject, None) {
        Ok(t) => t,
        Err(e) => {
            respond(stream, &e)?;
            return Ok(());
        }
    };
    let mut writer = stream.try_clone()?;
    let mut last_sent = after.unwrap_or(0);
    for (seq, event) in backlog {
        let line = json!({ "seq": seq, "event": event }).to_string();
        writeln!(writer, "{line}")?;
        last_sent = seq;
    }
    writer.flush()?;
    for line in rx {
        let seq = serde_json::from_str::<Value>(&line)
            .ok()
            .and_then(|v| v.get("seq").and_then(|s| s.as_u64()))
            .unwrap_or(u64::MAX);
        if seq <= last_sent {
            continue;
        }
        if writeln!(writer, "{line}").is_err() || writer.flush().is_err() {
            break; // client went away; retain() will drop the sender
        }
        last_sent = seq;
    }
    Ok(())
}

/// Register a subscription whose sink is a live listener, wherever that
/// listener is attached (unix socket or HTTP stream). Returns the sub id,
/// the ack to send first, and the delivery channel. Err carries the protocol
/// error Value to send back.
///
/// Resume order of precedence: explicit `after` beats the named cursor's
/// stored position. Replay runs AFTER the listener channel is attached and
/// under the same lock as registration, so there is no window where a live
/// event can slip between backlog and stream: no gap, no duplicate.
pub(crate) fn attach_listener(
    state: &Arc<Mutex<State>>,
    string: &str,
    options: &[String],
    client: &str,
    after: Option<u64>,
    cursor: Option<&str>,
    filter: Option<Subscription>,
) -> Result<(String, Value, mpsc::Receiver<String>), Value> {
    let (tx, rx) = mpsc::channel::<String>();
    let mut s = state.lock().unwrap();
    let stored_cursor = cursor.and_then(|c| s.cursors.get(c)).map(|e| e.seq);
    let resume_after = after.or(stored_cursor);
    let mut ack = match s.register_listener(string, options, client) {
        Ok(ack) => ack,
        Err(e) => return Err(err(e)),
    };
    let id = ack
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    s.listeners
        .insert(id.clone(), ListenerHandle { tx, filter });

    // Catch-up replay into the now-attached channel. Cursor resume (exact,
    // by seq) wins over the subscription's own `since` (fuzzy, by time).
    let sub = s.matcher.get(&id).cloned();
    let mut replayed = 0u64;
    let mut gap_expired = 0u64;
    if let Some(after) = resume_after {
        let backlog = s.read_events(Some(after));
        if let Some((first_seq, _)) = backlog.first() {
            // Retention already evaporated part of the gap.
            gap_expired = first_seq.saturating_sub(after + 1);
        }
        for (seq, event) in backlog {
            s.current_seq = Some(seq);
            if let Some(n) = s.try_deliver(&id, &event) {
                replayed += n;
            }
        }
    } else if let Some(replay) = sub.as_ref().and_then(|sub| sub.replay.clone()) {
        let cutoff = unix_to_ts(now_unix().saturating_sub(replay.lookback.secs()));
        let backlog = s.read_events(None);
        for (seq, event) in backlog {
            if event.ts >= cutoff {
                s.current_seq = Some(seq);
                if let Some(n) = s.try_deliver(&id, &event) {
                    replayed += n;
                }
            }
        }
    }

    // Ack tells the consumer exactly where it stands: the log head (start a
    // cursor even with no traffic), what was replayed, and what is gone.
    ack["seq"] = json!(s.next_seq.saturating_sub(1));
    ack["replayed"] = json!(replayed);
    if let Some(after) = resume_after {
        ack["resumedFrom"] = json!(after);
    }
    if gap_expired > 0 {
        ack["gapExpired"] = json!(gap_expired);
    }
    if let Some(name) = cursor {
        ack["cursor"] = json!(name);
    }
    let semantic_sub = sub
        .as_ref()
        .is_some_and(|sub| sub.meaning.is_some() || sub.judge.is_some());
    if semantic_sub && (resume_after.is_some() || replayed > 0) {
        if let Some(w) = ack.get_mut("warnings").and_then(|w| w.as_array_mut()) {
            w.push(json!(
                "catch-up replay evaluates tiers 1-2 only; meaning/judge verdicts \
                 were not recorded for this subscription while it was away"
            ));
        }
    }
    Ok((id, ack, rx))
}

/// Tear a listener's subscription down. Guarded: only the party that still
/// finds the listener does the removal, so racing detach paths (watchdog,
/// writer loop, HTTP stream end) can't double-remove.
pub(crate) fn detach_listener(state: &Arc<Mutex<State>>, sub_id: &str, reason: &str) {
    let mut s = state.lock().unwrap();
    if s.listeners.remove(sub_id).is_some() {
        s.remove_sub(sub_id, reason);
    }
}

/// A `Listen` connection: register the subscription with this connection as
/// its sink, ack with the id, then stream deliveries until the client hangs
/// up or the subscription is removed. Disconnect tears the subscription down
/// — this is the enforced form of the lease lifetime.
fn handle_listen(
    string: &str,
    options: &[String],
    client: Option<String>,
    after: Option<u64>,
    cursor: Option<String>,
    stream: &UnixStream,
    state: &Arc<Mutex<State>>,
) -> anyhow::Result<()> {
    let client = client.unwrap_or_else(|| "listener".to_string());
    // Unix socket = local operator = unfiltered.
    let (sub_id, ack, rx) = match attach_listener(
        state,
        string,
        options,
        &client,
        after,
        cursor.as_deref(),
        None,
    ) {
        Ok(attached) => attached,
        Err(e) => {
            respond(stream, &e)?;
            return Ok(());
        }
    };
    respond(stream, &ack)?;

    let teardown = |reason: &str| detach_listener(state, &sub_id, reason);

    // EOF watchdog: deliveries may be rare, so a broken pipe alone would
    // detect disconnects too late. A blocked read notices immediately.
    {
        let state = Arc::clone(state);
        let sub_id = sub_id.clone();
        let reader = stream.try_clone()?;
        std::thread::spawn(move || {
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {} // the connection is delivery-only; ignore input
                }
            }
            detach_listener(&state, &sub_id, "listener disconnected");
        });
    }

    let mut writer = stream.try_clone()?;
    for line in rx {
        if writeln!(writer, "{line}").is_err() || writer.flush().is_err() {
            teardown("listener disconnected");
            break;
        }
    }
    // Channel closed: the subscription was removed daemon-side (limit hit,
    // rm, ttl), the client hung up, or teardown already ran. The watchdog's
    // fd clone keeps the socket alive, so shut it down explicitly — that
    // sends EOF to the client and unblocks the watchdog's read.
    let _ = stream.shutdown(std::net::Shutdown::Both);
    Ok(())
}

fn respond(mut stream: &UnixStream, value: &Value) -> anyhow::Result<()> {
    let mut line = serde_json::to_string(value)?;
    line.push('\n');
    stream.write_all(line.as_bytes())?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod grant_tests {
    use super::*;

    fn ev(subject: &str, payload: Value) -> Envelope {
        Envelope {
            id: "E.1".into(),
            ts: "2026-08-12T00:00:00Z".into(),
            node: "n".into(),
            source: "s".into(),
            event_type: subject.into(),
            subject: subject.into(),
            correlation: None,
            payload,
            ttl_class: None,
            hops: None,
        }
    }

    #[test]
    fn grant_filters_are_deterministic_tiers_only() {
        assert!(parse_filter("on ci.**, deploy.*").is_ok());
        assert!(parse_filter(r#"on a.* where payload.env == "prod""#).is_ok());
        // Probabilistic tiers and actions have no place in authorization.
        assert!(parse_filter(r#"on a.* meaning "x""#).is_err());
        assert!(parse_filter("on a.* then cmd echo hi").is_err());
        assert!(parse_filter(r#"on a.* judge "q" budget 1/day"#).is_err());
    }

    #[test]
    fn grant_filters_match_like_the_matcher() {
        let f = parse_filter(r#"on ci.**, deploy.* where payload.env == "prod""#).unwrap();
        assert!(filter_matches(
            &f,
            &ev("deploy.done", json!({"env": "prod"}))
        ));
        assert!(filter_matches(
            &f,
            &ev("ci.run.completed", json!({"env": "prod"}))
        ));
        assert!(!filter_matches(
            &f,
            &ev("deploy.done", json!({"env": "dev"}))
        ));
        assert!(!filter_matches(
            &f,
            &ev("hr.salary", json!({"env": "prod"}))
        ));
    }

    #[test]
    fn grants_need_a_surface_and_real_tokens() {
        let def = GrantDef {
            name: "x".into(),
            token: "0123456789abcdef".into(),
            allow: None,
            emit: None,
        };
        assert!(def.compile().is_err());
        let def = GrantDef {
            name: "x".into(),
            token: "0123456789abcdef".into(),
            allow: Some("on a.*".into()),
            emit: None,
        };
        assert_eq!(def.fingerprint().len(), 12);
        assert!(def.compile().is_ok());
    }
}
