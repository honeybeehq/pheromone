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

struct State {
    paths: Paths,
    node: String,
    matcher: Matcher,
    metas: HashMap<String, SubMeta>,
    next_seq: u64,
    timers: Vec<Timer>,
    tails: Vec<TailClient>,
    retention_secs: u64,
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

        let mut state = State {
            paths,
            node,
            matcher,
            metas,
            next_seq,
            timers,
            tails: Vec::new(),
            retention_secs,
        };
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

        // Honesty gates: never accept what this build cannot evaluate or execute.
        if sub.meaning.is_some() || sub.judge.is_some() {
            return Err(
                "tiers 3-4 (meaning/judge) are not implemented yet (roadmap slices 4-5); \
                 this subscription would never fire — refusing to register it silently"
                    .to_string(),
            );
        }
        match sub.then.sink {
            Sink::Cmd | Sink::Emit | Sink::Buz => {}
            other => {
                return Err(format!(
                    "sink '{}' is not implemented yet in the prototype (available: cmd, emit, buz)",
                    other.name()
                ))
            }
        }
        if sub.then.sink == Sink::Buz && sub.then.args.is_empty() {
            return Err("'then buz' requires a bee name".to_string());
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
        let expires_at = match &sub.lifetime {
            pher_core::Lifetime::Ttl { ttl } => Some(now_unix() + ttl.secs()),
            _ => None,
        };
        let mut warnings: Vec<String> = Vec::new();
        if !matches!(sub.delivery, pher_core::Delivery::Immediate) {
            warnings.push(
                "delivery shaping (every/batch) is not enforced yet; delivering immediately"
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
        Ok((seq, deliveries))
    }

    /// Deliver `event` to subscription `sub_id` if its cascade matches.
    /// Returns Some(count) when a delivery happened.
    fn try_deliver(&mut self, sub_id: &str, event: &Envelope) -> Option<u64> {
        let sub = self.matcher.get(sub_id)?.clone();
        let eval = evaluate(sub_id, &sub, event, None);
        if !matches!(eval.outcome, Outcome::Matched) {
            return None;
        }
        let mut block = eval.match_block?;
        self.deliver(sub_id, &sub, event, &mut block);
        Some(1)
    }

    fn deliver(
        &mut self,
        sub_id: &str,
        sub: &Subscription,
        event: &Envelope,
        block: &mut MatchBlock,
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

        let sink_result = self.run_sink(sub, event, block, &delivery_id);
        let record = json!({
            "deliveryId": delivery_id,
            "ts": now_ts(),
            "event": event,
            "match": block,
            "sink": sink_result,
        });
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
    ) -> Value {
        let delivery_json = json!({ "event": event, "match": block });
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
            _ => json!({ "sink": sub.then.sink.name(), "error": "sink not implemented" }),
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
            let _ = self.persist_subs();
            let _ = self.persist_timers();
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
            self.deliver(&timer.sub_id.clone(), &sub, &timer.origin, &mut block);
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
        Ok(())
    }
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

    // Housekeeping: expect-timer expiry every second, GC sweep every 10 minutes.
    {
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            let mut last_gc = now_unix();
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let mut s = state.lock().unwrap();
                s.fire_expired_timers();
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

fn handle_connection(stream: UnixStream, state: Arc<Mutex<State>>) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(());
    }
    let request: Request = match serde_json::from_str(line.trim()) {
        Ok(r) => r,
        Err(e) => {
            respond(&stream, &err(format!("bad request: {e}")))?;
            return Ok(());
        }
    };

    match request {
        Request::Emit { event } => {
            let result = state.lock().unwrap().ingest(event, 0);
            respond(&stream, &result.unwrap_or_else(err))?;
        }
        Request::When { string, options } => {
            let result = state.lock().unwrap().register(&string, &options);
            respond(&stream, &result.unwrap_or_else(err))?;
        }
        Request::Ls => {
            let s = state.lock().unwrap();
            let mut subs: Vec<&SubMeta> = s.metas.values().collect();
            subs.sort_by(|a, b| a.created.cmp(&b.created));
            respond(&stream, &ok(json!({ "subs": subs })))?;
        }
        Request::Rm { id } => {
            let removed = state.lock().unwrap().remove_sub(&id, "removed by operator");
            respond(&stream, &ok(json!({ "removed": removed })))?;
        }
        Request::Status => {
            let s = state.lock().unwrap();
            respond(
                &stream,
                &ok(json!({
                    "node": s.node,
                    "subscriptions": s.matcher.len(),
                    "nextSeq": s.next_seq,
                    "armedTimers": s.timers.len(),
                    "tails": s.tails.len(),
                    "retention": format!("{}s", s.retention_secs),
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
                Some(r) => respond(&stream, &ok(json!({ "delivery": r })))?,
                None => respond(&stream, &err(format!("no delivery '{delivery_id}'")))?,
            }
        }
        Request::WhyNot { sub, event } => {
            let s = state.lock().unwrap();
            let Some(subscription) = s.matcher.get(&sub).cloned() else {
                let e = err(format!("no subscription '{sub}'"));
                drop(s);
                respond(&stream, &e)?;
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
                    let report = why_not(&sub, &subscription, &envelope);
                    respond(&stream, &ok(json!({ "report": report })))?;
                }
                None => respond(&stream, &err(format!("no event '{event}' in the log")))?,
            }
        }
        Request::Tail { after, subject } => {
            let pattern = match subject.map(|s| SubjectPattern::parse(&s)).transpose() {
                Ok(p) => p,
                Err(e) => {
                    respond(&stream, &err(e))?;
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
