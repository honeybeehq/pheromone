//! Ecosystem taps. The flagship is the hive ledger tap: the Honeybee ledger
//! stream (~160 well-namespaced event types, zero programmatic subscribers
//! until now) piped onto the bus as `hive.<type>` events.
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

/// How the tap reaches its bus: a persistent local socket, or per-event
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
/// The first one present becomes the envelope correlation, so trails join
/// spawn → prompt → seal across the whole vocabulary.
const CORRELATION_FIELDS: &[&str] = &["session", "bee", "name", "flight", "id"];

pub fn run_hive_tap(target: &Target, paths: &Paths, since: &str) -> anyhow::Result<()> {
    let mut backlog_since = since.to_string();
    loop {
        match follow_once(target, paths, &backlog_since) {
            Ok(()) => unreachable!("follow_once only returns by error"),
            Err(e) => eprintln!("pher tap hive: {e:#}; retrying in 2s"),
        }
        // After the first attempt never replay a long backlog again.
        backlog_since = "1s".to_string();
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn follow_once(target: &Target, paths: &Paths, since: &str) -> anyhow::Result<()> {
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

    let mut count: u64 = 0;
    for line in BufReader::new(stdout).lines() {
        let line = line.context("hive events stream read failed")?;
        let Some(event) = map_ledger_line(&line) else {
            continue;
        };
        if let Err(e) = conn.emit(&Request::Emit { event }) {
            let _ = child.kill();
            return Err(e).context("emit to pherd failed");
        }
        count += 1;
        if count.is_multiple_of(500) {
            eprintln!("pher tap hive: {count} events forwarded");
        }
    }
    let _ = child.wait();
    bail!("`hive events --follow` exited")
}

/// Map one ledger JSON line to a bus event. Returns None for unparseable
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
