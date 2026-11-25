use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::client::{self, Conn, Rejected, Target};
use crate::protocol::{PartialEvent, Request};
use crate::store::{write_json_atomic, Paths};

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
            event: tap_event(subject, payload),
        })
        .map(|_| ())
    }

    fn emit_once(&mut self, key: String, event: PartialEvent) -> anyhow::Result<()> {
        self.emit(&Request::EmitOnce { key, event }).map(|_| ())
    }
}

fn tap_event(subject: &str, payload: Value) -> PartialEvent {
    PartialEvent {
        subject: subject.to_string(),
        payload: Some(payload),
        event_type: None,
        source: Some(TAP_SOURCE.to_string()),
        correlation: None,
        source_ts: None,
    }
}

const TAP_SOURCE: &str = "tap.hive";
const HIVE_TAIL_MAX: usize = 1000;
const CURSOR_SAVE_EVERY: Duration = Duration::from_secs(1);

#[derive(Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
struct SavedCursor {
    ledger: String,
    after_seq: u64,
    after_ts: Option<u64>,
}

struct TapState {
    ledger: Option<String>,
    after_seq: Option<u64>,
    after_ts: Option<u64>,
    lookback: Duration,
    lookback_floor_ms: u64,
    forwarded: u64,
    cursor_file: PathBuf,
    saved: Option<SavedCursor>,
    saved_at: Instant,
}

impl TapState {
    fn load(paths: &Paths, target: &Target, lookback: Duration) -> anyhow::Result<TapState> {
        let cursor_file = paths.hive_tap_cursor(match target {
            Target::Local(_) => "local",
            Target::Remote { url, .. } => url.trim_end_matches('/'),
        });
        let saved = read_cursor(&cursor_file)?;
        let mut tap = TapState {
            ledger: saved.as_ref().map(|c| c.ledger.clone()),
            after_seq: saved.as_ref().map(|c| c.after_seq),
            after_ts: saved.as_ref().and_then(|c| c.after_ts),
            lookback,
            lookback_floor_ms: 0,
            forwarded: 0,
            cursor_file,
            saved,
            saved_at: Instant::now(),
        };
        if tap.after_seq.is_none() {
            tap.start_fresh();
        }
        Ok(tap)
    }

    fn start_fresh(&mut self) {
        self.after_seq = None;
        self.after_ts = None;
        self.lookback_floor_ms = now_ms().saturating_sub(self.lookback.as_millis() as u64);
    }

    fn restart_from_ledger_start(&mut self) {
        self.after_seq = Some(0);
        self.after_ts = None;
        self.lookback_floor_ms = 0;
    }

    fn advance(&mut self, row: &LedgerRow) {
        self.after_seq = Some(row.seq);
        self.after_ts = Some(row.ts_ms);
    }

    fn save(&mut self, force: bool) -> anyhow::Result<()> {
        let (Some(ledger), Some(after_seq)) = (self.ledger.clone(), self.after_seq) else {
            return Ok(());
        };
        let cursor = SavedCursor {
            ledger,
            after_seq,
            after_ts: self.after_ts,
        };
        if self.saved.as_ref() == Some(&cursor)
            || (!force && self.saved_at.elapsed() < CURSOR_SAVE_EVERY)
        {
            return Ok(());
        }
        write_json_atomic(&self.cursor_file, &serde_json::to_value(&cursor)?).with_context(
            || {
                format!(
                    "cannot save the tap cursor to {}",
                    self.cursor_file.display()
                )
            },
        )?;
        self.saved = Some(cursor);
        self.saved_at = Instant::now();
        Ok(())
    }

    fn row_key(&self, row: &LedgerRow) -> String {
        format!(
            "hive|{}|{}|{}",
            self.ledger.as_deref().unwrap_or_default(),
            row.seq,
            row.ts_ms
        )
    }
}

fn read_cursor(path: &PathBuf) -> anyhow::Result<Option<SavedCursor>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("unreadable tap cursor file {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("cannot read {}", path.display())),
    }
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
    let mut tap = TapState::load(paths, target, lookback)?;
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

    let mut attached = false;
    for line in BufReader::new(stdout).lines() {
        let line = line.context("hive events stream read failed")?;
        let Ok(value) = serde_json::from_str::<Value>(line.trim()) else {
            continue;
        };
        let rows = match value.get("rows").and_then(|r| r.as_array()) {
            Some(batch) if !attached => {
                let rows: Vec<LedgerRow> = batch.iter().filter_map(parse_row).collect();
                attach(tap, &value, &rows, since)?;
                attached = true;
                rows
            }
            _ if !attached => {
                if let Some(error) = value.get("error") {
                    bail!("`hive events` failed: {error}");
                }
                continue;
            }
            _ => parse_row(&value).into_iter().collect(),
        };
        for row in rows {
            forward_row(&mut conn, tap, excludes, row)?;
        }
    }
    tap.save(true)?;
    bail!("`hive events --follow` exited")
}

fn attach(
    tap: &mut TapState,
    batch: &Value,
    rows: &[LedgerRow],
    since: &str,
) -> anyhow::Result<()> {
    let ledger = batch
        .pointer("/daemonTarget/dataDir")
        .and_then(|d| d.as_str())
        .unwrap_or("default")
        .to_string();
    if tap.ledger.as_deref().is_some_and(|known| known != ledger) {
        eprintln!(
            "pher tap hive: the hive ledger moved from {} to {ledger}; following the new one (since {since})",
            tap.ledger.as_deref().unwrap_or_default()
        );
        tap.start_fresh();
    }
    tap.ledger = Some(ledger);
    match tap.after_seq {
        Some(after) if after > 0 && ledger_restarted(tap, rows, after) => {
            eprintln!(
                "pher tap hive: the hive ledger restarted below seq {after}; following it from the start"
            );
            tap.restart_from_ledger_start();
        }
        Some(after) => eprintln!("pher tap hive: resuming the hive ledger after seq {after}"),
        None => eprintln!("pher tap hive: following the hive ledger (since {since})"),
    }
    tap.save(true)
}

fn ledger_restarted(tap: &TapState, rows: &[LedgerRow], after: u64) -> bool {
    let newest_below = rows.last().is_none_or(|row| row.seq < after);
    let cursor_row_changed = rows
        .iter()
        .find(|row| row.seq == after)
        .zip(tap.after_ts)
        .is_some_and(|(row, ts)| row.ts_ms != ts);
    newest_below || cursor_row_changed
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
        "pher tap hive: {missed} ledger rows (seq {from_seq}..={to_seq}) are no longer readable through `hive events`"
    );
    let ledger = tap.ledger.clone().unwrap_or_default();
    conn.emit_once(
        format!("hive-gap|{ledger}|{from_seq}|{to_seq}"),
        tap_event(
            "pher.tap.gap",
            json!({ "tap": "hive", "ledger": ledger, "from_seq": from_seq, "to_seq": to_seq, "missed": missed }),
        ),
    )
    .context("emit to pherd failed")?;
    tap.after_seq = Some(to_seq);
    tap.after_ts = None;
    tap.save(true)
}

fn report_rejected(
    conn: &mut Sender,
    tap: &TapState,
    row: &LedgerRow,
    key: String,
    error: &anyhow::Error,
) -> anyhow::Result<()> {
    eprintln!(
        "pher tap hive: the trail refused ledger row {} ({}): {error}",
        row.seq, row.kind
    );
    conn.emit_once(
        format!("rejected|{key}"),
        tap_event(
            "pher.tap.rejected",
            json!({
                "tap": "hive",
                "ledger": tap.ledger,
                "seq": row.seq,
                "ts": row.ts_ms,
                "kind": row.kind,
                "beeId": row.value.get("beeId"),
                "error": error.to_string(),
            }),
        ),
    )
    .context("cannot report a refused ledger row")
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
        let key = tap.row_key(&row);
        match conn.emit_once(key.clone(), map_row(row.value.clone())) {
            Err(e) if e.is::<Rejected>() => report_rejected(conn, tap, &row, key, &e)?,
            result => result.context("emit to pherd failed")?,
        }
        tap.forwarded += 1;
        if tap.forwarded.is_multiple_of(500) {
            eprintln!("pher tap hive: {} events forwarded", tap.forwarded);
        }
    }
    tap.advance(&row);
    tap.save(false)
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
        source_ts: None,
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
