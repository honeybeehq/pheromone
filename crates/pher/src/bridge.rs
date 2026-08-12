//! Bridges: pull composition between buses. A bridge is a durable `/listen`
//! against an upstream bus — filtered by an ordinary subscription — whose
//! deliveries are re-ingested locally with envelope identity preserved
//! (same event id, origin node, timestamps; hops incremented). Robustness
//! is inherited wholesale from the listen/cursor machinery: the upstream
//! cursor advances only after local ingestion, so an outage replays exactly
//! the gap; admission dedups by event id, so overlap never duplicates;
//! reconnect uses backoff and resets on progress.
//!
//! The upstream evaluates the full cascade (its tiers, its budgets); this
//! side only admits. Composition without a federation protocol: the bus
//! graph is whatever bridges and forwards exist, and cycles die at the hop
//! cap.

use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::daemon::State;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeDef {
    pub name: String,
    /// Upstream base URL (resolved from the node registry at apply time).
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub token: Option<String>,
    /// Subscription text; `then stream` is implied.
    pub sub: String,
    /// Upstream cursor name; defaults to `bridge:<name>@<local node>`.
    #[serde(default)]
    pub cursor: String,
}

impl BridgeDef {
    pub fn sub_text(&self) -> String {
        if pher_core::Subscription::parse(&self.sub).is_ok() {
            self.sub.clone()
        } else {
            format!("{} then stream", self.sub)
        }
    }
}

/// Run a bridge until its cancel flag flips. Spawned per bridge by the
/// daemon (at startup for persisted bridges, at BridgeAdd for new ones).
pub(crate) fn spawn(state: Arc<Mutex<State>>, def: BridgeDef, cancel: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let mut delay = 2u64;
        loop {
            if cancel.load(Ordering::Relaxed) {
                return;
            }
            let mut progressed = false;
            match follow(&state, &def, &cancel, &mut progressed) {
                Ok(()) => return, // cancelled cleanly
                Err(e) => eprintln!("pherd bridge '{}': {e:#}; retrying in {delay}s", def.name),
            }
            std::thread::sleep(Duration::from_secs(delay));
            delay = if progressed { 2 } else { (delay * 2).min(30) };
        }
    });
}

fn follow(
    state: &Arc<Mutex<State>>,
    def: &BridgeDef,
    cancel: &Arc<AtomicBool>,
    progressed: &mut bool,
) -> anyhow::Result<()> {
    let body = json!({
        "string": def.sub_text(),
        "client": format!("bridge:{}", def.name),
        "cursor": def.cursor,
    });
    let mut req =
        ureq::post(&format!("{}/listen", def.url)).set("content-type", "application/json");
    if let Some(token) = &def.token {
        req = req.set("authorization", &format!("Bearer {token}"));
    }
    let response = req.send_string(&body.to_string()).map_err(|e| match e {
        ureq::Error::Status(code, resp) => {
            let text = resp.into_string().unwrap_or_default();
            anyhow::anyhow!("upstream refused listen ({code}): {text}")
        }
        other => anyhow::anyhow!("upstream unreachable at {}: {other}", def.url),
    })?;

    let reader = BufReader::new(response.into_reader());
    for line in reader.lines() {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        let line = line.context("upstream stream read failed")?;
        let line = line.trim();
        if line.is_empty() {
            continue; // heartbeat
        }
        let v: Value = serde_json::from_str(line).context("bad line from upstream")?;
        if v.get("ok").and_then(|o| o.as_bool()) == Some(false) {
            bail!(
                "upstream error: {}",
                v.get("error").and_then(|e| e.as_str()).unwrap_or("unknown")
            );
        }
        if let Some(canonical) = v.get("canonical").and_then(|c| c.as_str()) {
            // The registration ack.
            let replayed = v.get("replayed").and_then(|r| r.as_u64()).unwrap_or(0);
            eprintln!(
                "pherd bridge '{}': attached upstream ({canonical}), {replayed} replayed",
                def.name
            );
            if let Some(gap) = v.get("gapExpired").and_then(|g| g.as_u64()) {
                eprintln!(
                    "pherd bridge '{}': warning: {gap} upstream event(s) in the gap already evaporated",
                    def.name
                );
            }
            continue;
        }
        let Some(event) = v.get("event") else {
            continue;
        };
        let event: pher_core::Envelope =
            serde_json::from_value(event.clone()).context("bad envelope from upstream")?;
        let admitted = {
            let mut s = state.lock().unwrap();
            s.admit_remote(event)
        };
        match admitted {
            Ok(_) => *progressed = true,
            Err(e) => eprintln!("pherd bridge '{}': event rejected: {e}", def.name),
        }
        // Commit AFTER local ingestion: crash between ingest and commit
        // means redelivery, and admission dedup makes redelivery a no-op —
        // at-least-once upstream, effectively-once here.
        if let Some(seq) = v.get("seq").and_then(|s| s.as_u64()) {
            let commit = json!({ "op": "cursorCommit", "name": def.cursor, "seq": seq });
            let mut req = ureq::post(&format!("{}/rpc", def.url))
                .timeout(Duration::from_secs(10))
                .set("content-type", "application/json");
            if let Some(token) = &def.token {
                req = req.set("authorization", &format!("Bearer {token}"));
            }
            let _ = req.send_string(&commit.to_string()); // best-effort; dedup covers loss
        }
    }
    bail!("upstream closed the stream")
}
