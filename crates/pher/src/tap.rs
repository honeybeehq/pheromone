//! Ecosystem taps. The flagship is the hive ledger tap: the Honeybee ledger
//! stream (~160 well-namespaced event types, zero programmatic subscribers
//! until now) piped onto the trail as `hive.<type>` events.
//!
//! Prototype note: this consumes `hive events --follow --json` (the CLI is the
//! stable read surface and works even while the hive daemon is down) rather
//! than importing the TS `followLedgerEvents` module; the TS-module tap
//! arrives with the `@pheromone/client` SDK.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Context};
use serde_json::{json, Value};

use crate::client::{self, Conn, Target};
use crate::protocol::{PartialEvent, Request};
use crate::store::Paths;

/// How the tap reaches its trail: a persistent local socket, or per-event
/// HTTP to a remote hub (leaf machines need no local daemon at all).
enum Sender<'a> {
    Local(Conn),
    Remote(&'a Target),
}

impl Sender<'_> {
    fn emit(&mut self, request: &Request) -> anyhow::Result<serde_json::Value> {
        match self {
            Sender::Local(conn) => conn.call(request),
            Sender::Remote(target) => client::call_target(target, request),
        }
    }
}

/// Ledger fields that identify the acting session/bee, in preference order.
/// The first one present becomes the envelope correlation, so threads join
/// spawn → prompt → seal across the whole vocabulary.
const CORRELATION_FIELDS: &[&str] = &["session", "bee", "name", "flight", "id"];

/// Reconnect bookkeeping: where the tap was in the ledger, and what it has
/// already forwarded. `seen` dedups the resume overlap — re-emitting a
/// ledger line would mint a fresh envelope id, so without this, retries
/// would duplicate events on the trail.
struct TapState {
    /// Unix secs of the last forwarded ledger event.
    last_ts: Option<u64>,
    /// Recent raw ledger lines (the ledger is append-only; identical lines
    /// mean the same event).
    seen: std::collections::VecDeque<String>,
    forwarded: u64,
}

const TAP_SEEN_WINDOW: usize = 1000;

pub fn run_hive_tap(
    target: &Target,
    paths: &Paths,
    since: &str,
    excludes: &[String],
) -> anyhow::Result<()> {
    let mut tap = TapState {
        last_ts: None,
        seen: std::collections::VecDeque::new(),
        forwarded: 0,
    };
    if !excludes.is_empty() {
        eprintln!(
            "pher tap hive: dropping ledger types: {}",
            excludes.join(", ")
        );
    }
    let mut backlog_since = since.to_string();
    let mut delay = 2u64;
    loop {
        let before = tap.forwarded;
        match follow_once(target, paths, &backlog_since, excludes, &mut tap) {
            Ok(()) => unreachable!("follow_once only returns by error"),
            Err(e) => eprintln!("pher tap hive: {e:#}; retrying in {delay}s"),
        }
        // Resume from the last forwarded event (with a 2s overlap the seen-
        // window dedups), so an outage delays ledger events instead of
        // dropping them. Cap the re-read at a day.
        backlog_since = match tap.last_ts {
            Some(t) => {
                let gap = (now_unix().saturating_sub(t) + 2).min(86_400);
                format!("{gap}s")
            }
            None => backlog_since, // never connected: keep the requested lookback
        };
        std::thread::sleep(Duration::from_secs(delay));
        // Progress resets the backoff; persistent failure walks it to 30s.
        delay = if tap.forwarded > before {
            2
        } else {
            (delay * 2).min(30)
        };
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn follow_once(
    target: &Target,
    paths: &Paths,
    since: &str,
    excludes: &[String],
    tap: &mut TapState,
) -> anyhow::Result<()> {
    let mut conn = match target {
        Target::Local(_) => Sender::Local(Conn::connect(paths)?),
        remote => Sender::Remote(remote),
    };
    let mut child = Command::new("hive")
        .args(["events", "--follow", "--json", "--since", since])
        .stdout(Stdio::piped())
        .stdin(Stdio::null())
        .spawn()
        .context("cannot spawn `hive events` (is the hive CLI installed?)")?;
    let stdout = child.stdout.take().expect("stdout piped");

    let announce = conn.emit(&Request::Emit {
        event: PartialEvent {
            subject: "pher.tap.up".to_string(),
            payload: Some(json!({ "tap": "hive", "since": since })),
            event_type: None,
            source: Some("tap.hive".to_string()),
            correlation: None,
        },
    });
    if let Err(e) = announce {
        let _ = child.kill();
        return Err(e).context("cannot reach pherd");
    }
    eprintln!("pher tap hive: following the hive ledger (since {since})");

    for line in BufReader::new(stdout).lines() {
        let line = line.context("hive events stream read failed")?;
        let trimmed = line.trim().to_string();
        let Some(event) = map_ledger_line(&trimmed) else {
            continue;
        };
        // Edge filtering: heartbeat-grade ledger types (e.g. state.verified
        // liveness probes at hundreds/min) never reach the trail or its log.
        let typ = event
            .subject
            .strip_prefix("hive.")
            .unwrap_or(&event.subject);
        if excludes.iter().any(|x| typ.starts_with(x.as_str())) {
            if let Some(ts) = ledger_ts(&trimmed) {
                tap.last_ts = Some(ts); // resume position still advances
            }
            continue;
        }
        if tap.seen.contains(&trimmed) {
            continue; // resume overlap: already forwarded before the retry
        }
        if let Err(e) = conn.emit(&Request::Emit { event }) {
            let _ = child.kill();
            return Err(e).context("emit to pherd failed");
        }
        tap.seen.push_back(trimmed.clone());
        while tap.seen.len() > TAP_SEEN_WINDOW {
            tap.seen.pop_front();
        }
        if let Some(ts) = ledger_ts(&trimmed) {
            tap.last_ts = Some(ts);
        }
        tap.forwarded += 1;
        if tap.forwarded.is_multiple_of(500) {
            eprintln!("pher tap hive: {} events forwarded", tap.forwarded);
        }
    }
    let _ = child.wait();
    bail!("`hive events --follow` exited")
}

/// Unix secs of a ledger line's `ts` field (RFC3339), if parseable.
fn ledger_ts(line: &str) -> Option<u64> {
    let v: Value = serde_json::from_str(line).ok()?;
    let ts = v.get("ts")?.as_str()?;
    humantime::parse_rfc3339_weak(ts)
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
}

/// Map one ledger JSON line to a trail event. Returns None for unparseable
/// lines (the ledger is append-only JSONL; torn lines happen at rotation).
fn map_ledger_line(line: &str) -> Option<PartialEvent> {
    let v: Value = serde_json::from_str(line.trim()).ok()?;
    let obj = v.as_object()?;
    let typ = obj
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("unknown");
    let subject = format!("hive.{}", sanitize_subject(typ));
    let correlation = CORRELATION_FIELDS
        .iter()
        .find_map(|k| obj.get(*k).and_then(|x| x.as_str()))
        .map(|s| s.to_string());
    Some(PartialEvent {
        event_type: Some(subject.clone()),
        subject,
        payload: Some(v),
        source: Some("tap.hive".to_string()),
        correlation,
    })
}

/// Ledger types are dotted already (`session.save`, `spawn.timing`); replace
/// anything outside the subject-token alphabet so exotic types can't break
/// subject parsing.
fn sanitize_subject(typ: &str) -> String {
    typ.split('.')
        .map(|tok| {
            let cleaned: String = tok
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                        c
                    } else {
                        '-'
                    }
                })
                .collect();
            if cleaned.is_empty() {
                "unknown".to_string()
            } else {
                cleaned
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_ledger_lines() {
        let e = map_ledger_line(
            r#"{"ts":"2026-08-09T08:15:23.993Z","type":"session.save","name":"CO.0af3","status":"running","session":"CO.0af3"}"#,
        )
        .unwrap();
        assert_eq!(e.subject, "hive.session.save");
        assert_eq!(e.correlation.as_deref(), Some("CO.0af3"));
        assert_eq!(e.source.as_deref(), Some("tap.hive"));

        assert!(map_ledger_line("not json").is_none());
        assert!(map_ledger_line("").is_none());

        let e = map_ledger_line(r#"{"type":"weird$type.x!","ts":"t"}"#).unwrap();
        assert_eq!(e.subject, "hive.weird-type.x-");
    }
}
