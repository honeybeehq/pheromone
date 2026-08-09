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
    /// Events collapsed (debounce) or dropped over the queue cap (batch).
    #[serde(default)]
    collapsed: u64,
}

/// Evaporation bound for batch queues (principle 6: no unbounded buffers).
const MAX_BATCH_QUEUE: usize = 1000;

struct State {
    paths: Paths,
    node: String,
    matcher: Matcher,
    metas: HashMap<String, SubMeta>,
    next_seq: u64,
    timers: Vec<Timer>,
    pending: HashMap<String, PendingWindow>,
    tails: Vec<TailClient>,
    retention_secs: u64,
    semantic: crate::semantic::Semantic,
    judge_cfg: Option<crate::judge::JudgeConfig>,
    judge_tx: Option<mpsc::Sender<JudgeJob>>,
    judge_budgets: HashMap<String, JudgeBudget>,
    verdict_cache: HashMap<String, crate::judge::Verdict>,
}

/// Per-subscription judge budget window. Persisted; fail-closed on exhaustion.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct JudgeBudget {
    #[serde(rename = "periodStart")]
    period_start: u64,
    used: u64,
    #[serde(default)]
    notified: bool,
}

/// A pending tier-4 evaluation, processed off-lock by the judge worker.
struct JudgeJob {
    sub_id: String,
    event: Envelope,
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
            retention_secs,
            semantic,
            judge_cfg: None,
            judge_tx: None,
            judge_budgets,
            verdict_cache,
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

    fn register(&mut self, string: &str, option_words: &[String]) -> Result<Value, String> {
        let mut sub = Subscription::parse(string).map_err(|e| e.to_string())?;
        if !option_words.is_empty() {
            pher_core::subscription::apply_option_words(&mut sub, option_words)
                .map_err(|e| e.to_string())?;
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
        if matches!(sub.lifetime, pher_core::Lifetime::Lease { .. }) {
            warnings.push(
                "lease liveness (while ... alive) is not heartbeat-checked yet; \
                 treat as durable until slice 3"
                    .to_string(),
            );
        }

        let meta = SubMeta {
            id: id.clone(),
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
        let mut replayed = 0u64;
        if let Some(replay) = sub.replay.clone() {
            let cutoff = unix_to_ts(now_unix().saturating_sub(replay.lookback.secs()));
            let backlog = self.read_events(None);
            for (_, event) in backlog {
                if event.ts >= cutoff {
                    if let Some(n) = self.try_deliver(&id, &event) {
                        replayed += n;
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

    fn ingest(&mut self, partial: PartialEvent, hops: u32) -> Result<Value, String> {
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
                if let Some(p) = self.pending.get_mut(sub_id) {
                    // Window open: collapse to the latest event, deliver at close.
                    p.events = vec![event.clone()];
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
                            collapsed: 0,
                        },
                    );
                    let _ = self.persist_pending();
                    1
                }
            }
            pher_core::Delivery::Batch { window } => {
                let p = self
                    .pending
                    .entry(sub_id.to_string())
                    .or_insert_with(|| PendingWindow {
                        mode: "batch".to_string(),
                        window_end: now_unix() + window.secs(),
                        events: Vec::new(),
                        collapsed: 0,
                    });
                if p.events.len() >= MAX_BATCH_QUEUE {
                    p.events.remove(0);
                    p.collapsed += 1;
                }
                p.events.push(event.clone());
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
                // dispatched off-thread so a slow endpoint never stalls ingest.
                // Failures come back onto the bus as pher.delivery.failed.
                let method = sub.then.args[0].to_uppercase();
                let url = sub.then.args[1].clone();
                let (method_r, url_r) = (method.clone(), url.clone());
                let body = delivery_json.to_string();
                let paths = self.paths.clone();
                let delivery_id = delivery_id.to_string();
                std::thread::spawn(move || {
                    let result = ureq::request(&method, &url)
                        .timeout(Duration::from_secs(10))
                        .set("content-type", "application/json")
                        .send_string(&body);
                    if let Err(e) = result {
                        report_delivery_failure(&paths, &delivery_id, "http", &e.to_string());
                    }
                });
                json!({ "sink": "http", "method": method_r, "url": url_r, "dispatched": true })
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

/// Report an async sink failure back onto the bus (from a sink thread, via
/// the daemon's own socket — the bus eats its own dog food).
fn report_delivery_failure(paths: &Paths, delivery_id: &str, sink: &str, error: &str) {
    let report = crate::protocol::Request::Emit {
        event: PartialEvent {
            subject: "pher.delivery.failed".to_string(),
            payload: Some(json!({
                "deliveryId": delivery_id,
                "sink": sink,
                "error": error,
            })),
            event_type: None,
            source: Some("pherd".to_string()),
            correlation: None,
        },
    };
    if let Ok(mut conn) = crate::client::Conn::connect(paths) {
        let _ = conn.call(&report);
    }
    eprintln!("pherd: delivery {delivery_id} via {sink} failed: {error}");
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

fn hostname() -> String {
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
        let streaming = matches!(request, Request::Tail { .. });
        handle_request(request, &stream, &state)?;
        if streaming {
            return Ok(());
        }
    }
}

fn handle_request(
    request: Request,
    stream: &UnixStream,
    state: &Arc<Mutex<State>>,
) -> anyhow::Result<()> {
    match request {
        Request::Emit { event } => {
            let result = state.lock().unwrap().ingest(event, 0);
            respond(stream, &result.unwrap_or_else(err))?;
        }
        Request::When { string, options } => {
            let result = state.lock().unwrap().register(&string, &options);
            respond(stream, &result.unwrap_or_else(err))?;
        }
        Request::Ls => {
            let s = state.lock().unwrap();
            let mut subs: Vec<&SubMeta> = s.metas.values().collect();
            subs.sort_by(|a, b| a.created.cmp(&b.created));
            respond(stream, &ok(json!({ "subs": subs })))?;
        }
        Request::Rm { id } => {
            let removed = state.lock().unwrap().remove_sub(&id, "removed by operator");
            respond(stream, &ok(json!({ "removed": removed })))?;
        }
        Request::Status => {
            let s = state.lock().unwrap();
            respond(
                stream,
                &ok(json!({
                    "node": s.node,
                    "subscriptions": s.matcher.len(),
                    "nextSeq": s.next_seq,
                    "armedTimers": s.timers.len(),
                    "tails": s.tails.len(),
                    "retention": format!("{}s", s.retention_secs),
                    "semantic": s.semantic.status(),
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
                })),
            )?;
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
                Some(r) => respond(stream, &ok(json!({ "delivery": r })))?,
                None => respond(stream, &err(format!("no delivery '{delivery_id}'")))?,
            }
        }
        Request::WhyNot { sub, event } => {
            let s = state.lock().unwrap();
            let Some(subscription) = s.matcher.get(&sub).cloned() else {
                let e = err(format!("no subscription '{sub}'"));
                drop(s);
                respond(stream, &e)?;
                return Ok(());
            };
            let found = s
                .read_events(None)
                .into_iter()
                .map(|(_, e)| e)
                .find(|e| e.id == event);
            drop(s);
            match found {
                Some(envelope) => {
                    let mut report = why_not(&sub, &subscription, &envelope);
                    // Tier 3/4 diagnosis: if tiers 1-2 passed, actually score
                    // the meaning clause, and report the judge's cached verdict
                    // (why-not never spends judge budget).
                    let pending_semantic = report["matched"] == json!(false)
                        && report["rejectedAt"] == json!(null)
                        && (subscription.meaning.is_some() || subscription.judge.is_some());
                    if pending_semantic {
                        let mut s = state.lock().unwrap();
                        let mut meaning_result: Option<(bool, Value)> = None;
                        if let Some(meaning) = subscription.meaning.clone() {
                            match s.semantic.embed_event(&envelope) {
                                Ok(vec) => {
                                    meaning_result = Some(s.semantic.score(
                                        &sub,
                                        &meaning,
                                        &envelope.id,
                                        &vec,
                                        now_unix(),
                                    ));
                                }
                                Err(e) => {
                                    report["reason"] =
                                        json!(format!("meaning tier unavailable: {e}"));
                                }
                            }
                        }
                        let meaning_passed = meaning_result.as_ref().map(|(p, _)| *p);
                        let meaning_record = meaning_result.map(|(_, r)| r);
                        if meaning_passed == Some(false) {
                            drop(s);
                            report = json!({
                                "subscription": sub,
                                "event": envelope.id,
                                "matched": false,
                                "rejectedAt": "meaning",
                                "meaning": meaning_record,
                            });
                        } else if subscription.judge.is_some() {
                            let key =
                                format!("{sub}:{}", crate::judge::content_fingerprint(&envelope));
                            let cached = s.verdict_cache.get(&key).cloned();
                            let model = s
                                .judge_cfg
                                .as_ref()
                                .map(|c| c.model.clone())
                                .unwrap_or_else(|| "unconfigured".to_string());
                            drop(s);
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
                            drop(s);
                            report = json!({
                                "subscription": sub,
                                "event": envelope.id,
                                "matched": true,
                                "rejectedAt": null,
                                "meaning": record,
                            });
                        } else {
                            drop(s);
                        }
                    }
                    respond(stream, &ok(json!({ "report": report })))?;
                }
                None => respond(stream, &err(format!("no event '{event}' in the log")))?,
            }
        }
        Request::Tail { after, subject } => {
            let pattern = match subject.map(|s| SubjectPattern::parse(&s)).transpose() {
                Ok(p) => p,
                Err(e) => {
                    respond(stream, &err(e))?;
                    return Ok(());
                }
            };
            let (tx, rx) = mpsc::channel::<String>();
            // Register the live feed first, then send the backlog, then drain
            // live messages skipping anything already sent (seq dedup).
            let backlog = {
                let mut s = state.lock().unwrap();
                s.tails.push(TailClient {
                    tx,
                    subject: pattern.clone(),
                });
                s.read_events(after)
            };
            let mut writer = stream.try_clone()?;
            let mut last_sent = after.unwrap_or(0);
            for (seq, event) in backlog {
                if let Some(p) = &pattern {
                    if !p.matches(&event.subject) {
                        continue;
                    }
                }
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
        }
    }
    Ok(())
}

fn respond(mut stream: &UnixStream, value: &Value) -> anyhow::Result<()> {
    let mut line = serde_json::to_string(value)?;
    line.push('\n');
    stream.write_all(line.as_bytes())?;
    stream.flush()?;
    Ok(())
}
