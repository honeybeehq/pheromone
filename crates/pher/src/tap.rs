use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Context};
use serde_json::{json, Value};

use crate::client::{self, Conn, Target};
use crate::protocol::{PartialEvent, Request};
use crate::store::Paths;

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

    fn emit_event(&mut self, subject: &str, payload: Value) -> anyhow::Result<()> {
        self.emit(&Request::Emit {
            event: PartialEvent {
                subject: subject.to_string(),
                payload: Some(payload),
                event_type: None,
                source: Some(TAP_SOURCE.to_string()),
                correlation: None,
            },
        })
        .map(|_| ())
    }
}

const TAP_SOURCE: &str = "tap.hive";
const HIVE_TAIL_MAX: usize = 1000;

struct TapState {
    after_seq: Option<u64>,
    lookback_floor_ms: u64,
    forwarded: u64,
}

struct HiveEvents(Child);

impl Drop for HiveEvents {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct LedgerRow {
    seq: u64,
    ts_ms: u64,
    kind: String,
    value: Value,
}

pub fn run_hive_tap(
    target: &Target,
    paths: &Paths,
    since: &str,
    excludes: &[String],
) -> anyhow::Result<()> {
    let lookback = humantime::parse_duration(since)
        .with_context(|| format!("--since {since:?} is not a duration like 30s or 15m"))?;
    let mut tap = TapState {
        after_seq: None,
        lookback_floor_ms: now_ms().saturating_sub(lookback.as_millis() as u64),
        forwarded: 0,
    };
    if !excludes.is_empty() {
        eprintln!(
            "pher tap hive: dropping ledger kinds: {}",
            excludes.join(", ")
        );
    }
    let mut delay = 2u64;
    loop {
        let before = tap.after_seq;
        let Err(e) = follow_once(target, paths, since, excludes, &mut tap);
        eprintln!("pher tap hive: {e:#}; retrying in {delay}s");
        std::thread::sleep(Duration::from_secs(delay));
        delay = if tap.after_seq > before {
            2
        } else {
            (delay * 2).min(30)
        };
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn follow_once(
    target: &Target,
    paths: &Paths,
    since: &str,
    excludes: &[String],
    tap: &mut TapState,
) -> anyhow::Result<std::convert::Infallible> {
    let mut conn = match target {
        Target::Local(_) => Sender::Local(Conn::connect(paths)?),
        remote => Sender::Remote(remote),
    };
    let mut child = HiveEvents(
        Command::new("hive")
            .args([
                "events",
                "--follow",
                "--json",
                "--tail",
                &HIVE_TAIL_MAX.to_string(),
            ])
            .stdout(Stdio::piped())
            .stdin(Stdio::null())
            .spawn()
            .context("cannot spawn `hive events` (is the hive CLI installed?)")?,
    );
    let stdout = child.0.stdout.take().expect("stdout piped");

    conn.emit_event(
        "pher.tap.up",
        json!({ "tap": "hive", "since": since, "after_seq": tap.after_seq }),
    )
    .context("cannot reach pherd")?;
    match tap.after_seq {
        Some(seq) => eprintln!("pher tap hive: resuming the hive ledger after seq {seq}"),
        None => eprintln!("pher tap hive: following the hive ledger (since {since})"),
    }

    let mut first_line = true;
    for line in BufReader::new(stdout).lines() {
        let line = line.context("hive events stream read failed")?;
        let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
            continue;
        };
        let rows = match value.get("rows").and_then(|r| r.as_array()) {
            Some(batch) if first_line => batch.iter().filter_map(parse_row).collect(),
            _ => parse_row(&value).into_iter().collect::<Vec<_>>(),
        };
        if first_line {
            first_line = false;
            detect_ledger_reset(tap, &rows);
        }
        for row in rows {
            forward_row(&mut conn, tap, excludes, row)?;
        }
    }
    bail!("`hive events --follow` exited")
}

fn detect_ledger_reset(tap: &mut TapState, rows: &[LedgerRow]) {
    let Some(after) = tap.after_seq else {
        return;
    };
    let newest = rows.last().map(|row| row.seq);
    if newest.is_none_or(|seq| seq < after) {
        eprintln!(
            "pher tap hive: the hive ledger restarted below seq {after} (newest is {newest:?}); following it from the start"
        );
        tap.after_seq = None;
    }
}

fn report_gap(conn: &mut Sender, tap: &mut TapState, next_seq: u64) -> anyhow::Result<()> {
    let Some(after) = tap.after_seq else {
        return Ok(());
    };
    if next_seq <= after + 1 {
        return Ok(());
    }
    let (from_seq, to_seq) = (after + 1, next_seq - 1);
    let missed = to_seq - from_seq + 1;
    eprintln!(
        "pher tap hive: {missed} ledger rows (seq {from_seq}..={to_seq}) were not delivered by `hive events` (more than {HIVE_TAIL_MAX} rows arrived at once)"
    );
    conn.emit_event(
        "pher.tap.gap",
        json!({ "tap": "hive", "from_seq": from_seq, "to_seq": to_seq, "missed": missed }),
    )
    .context("emit to pherd failed")?;
    tap.after_seq = Some(to_seq);
    Ok(())
}

fn forward_row(
    conn: &mut Sender,
    tap: &mut TapState,
    excludes: &[String],
    row: LedgerRow,
) -> anyhow::Result<()> {
    if tap.after_seq.is_some_and(|after| row.seq <= after) {
        return Ok(());
    }
    report_gap(conn, tap, row.seq)?;
    let wanted = row.ts_ms >= tap.lookback_floor_ms
        && !excludes.iter().any(|x| row.kind.starts_with(x.as_str()));
    if wanted {
        conn.emit(&Request::Emit {
            event: map_row(row.value),
        })
        .context("emit to pherd failed")?;
        tap.forwarded += 1;
        if tap.forwarded.is_multiple_of(500) {
            eprintln!("pher tap hive: {} events forwarded", tap.forwarded);
        }
    }
    tap.after_seq = Some(row.seq);
    Ok(())
}

fn parse_row(value: &Value) -> Option<LedgerRow> {
    let obj = value.as_object()?;
    let seq = obj.get("seq")?.as_u64()?;
    let ts_ms = obj.get("ts")?.as_u64()?;
    let kind = obj.get("kind")?.as_str()?.to_string();
    let mut row = obj.clone();
    row.remove("daemonTarget");
    Some(LedgerRow {
        seq,
        ts_ms,
        kind,
        value: Value::Object(row),
    })
}

fn map_row(row: Value) -> PartialEvent {
    let kind = row
        .get("kind")
        .and_then(|k| k.as_str())
        .unwrap_or("unknown");
    let subject = format!("hive.{}", sanitize_subject(kind));
    let correlation = row
        .get("beeId")
        .and_then(|b| b.as_str())
        .or_else(|| row.get("payload")?.get("beeId")?.as_str())
        .map(str::to_string);
    PartialEvent {
        event_type: Some(subject.clone()),
        subject,
        payload: Some(row),
        source: Some(TAP_SOURCE.to_string()),
        correlation,
    }
}

fn sanitize_subject(kind: &str) -> String {
    kind.split('.')
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
    fn maps_v2_ledger_rows() {
        let line = r#"{"seq":2771725,"ts":1795592324497,"kind":"runtime.updated","beeId":"e0115ec9","payload":{"runtime":{"state":"running"}},"daemonTarget":{"kind":"local"}}"#;
        let row = parse_row(&serde_json::from_str(line).unwrap()).unwrap();
        assert_eq!(row.seq, 2771725);
        assert_eq!(row.ts_ms, 1795592324497);
        let e = map_row(row.value);
        assert_eq!(e.subject, "hive.runtime.updated");
        assert_eq!(e.correlation.as_deref(), Some("e0115ec9"));
        assert_eq!(e.source.as_deref(), Some("tap.hive"));
        let payload = e.payload.unwrap();
        assert!(payload.get("daemonTarget").is_none());
        assert_eq!(payload["payload"]["runtime"]["state"], "running");

        let nested =
            r#"{"seq":1,"ts":1,"kind":"account_limits.put","beeId":null,"payload":{"beeId":"b1"}}"#;
        let row = parse_row(&serde_json::from_str(nested).unwrap()).unwrap();
        assert_eq!(map_row(row.value).correlation.as_deref(), Some("b1"));

        let batch = r#"{"rows":[],"daemonTarget":{}}"#;
        assert!(parse_row(&serde_json::from_str(batch).unwrap()).is_none());
        let v1 = r#"{"ts":"2026-08-09T08:15:23.993Z","type":"session.save"}"#;
        assert!(parse_row(&serde_json::from_str(v1).unwrap()).is_none());

        assert_eq!(sanitize_subject("weird$kind.x!"), "weird-kind.x-");
    }
}
