//! Sync TrailLog: the daemon's event log boundary.
//!
//! SqliteLog is the only writer of `events` rows and of `log_state` (head,
//! floor). Sequence numbers are assigned here, never by callers. Head survives
//! a full trim so the next append cannot reuse a seq.

use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use pher_core::Envelope;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::Value;

use crate::store::Paths;

pub const LOCAL_PARTITION: u32 = 0;
pub const MAX_READ: usize = 4096;
/// Daemon catch-up / tail page size. Must be in 1..=MAX_READ.
pub const LOG_PAGE: usize = 256;
/// SQLite INTEGER is signed. Seq values above this cannot be stored.
pub const SEQ_MAX: u64 = i64::MAX as u64;
/// Explicit short timeout for each first-open attempt. rusqlite 0.32.1
/// already installs 5000ms in Connection::open
/// (`inner_connection.rs:119`, `busy.rs:25/55`). WAL conversion can still
/// return SQLITE_BUSY without invoking that handler, so `retry_busy` wraps
/// the attempt.
pub(crate) const INIT_BUSY_TIMEOUT: Duration = Duration::from_millis(200);
/// Writer operations after init (append, seed retry leftover).
pub(crate) const WRITER_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const INIT_ATTEMPTS: u32 = 32;
const INIT_BACKOFF_CAP: Duration = Duration::from_millis(50);

pub(crate) fn sqlite_busy_or_locked(err: &rusqlite::Error) -> bool {
    matches!(
        err.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

pub(crate) fn is_busy_or_locked_anyhow(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<rusqlite::Error>()
            .is_some_and(sqlite_busy_or_locked)
    })
}

/// Bounded typed retry for first-open WAL/schema. The installed busy
/// handler (rusqlite 0.32.1 default 5000ms, plus INIT_BUSY_TIMEOUT) is
/// not always invoked when WAL conversion returns SQLITE_BUSY.
pub(crate) fn retry_busy<T>(mut once: impl FnMut() -> anyhow::Result<T>) -> anyhow::Result<T> {
    let mut backoff = Duration::from_millis(1);
    let mut last = None;
    for attempt in 1..=INIT_ATTEMPTS {
        match once() {
            Ok(v) => return Ok(v),
            Err(e) if is_busy_or_locked_anyhow(&e) && attempt < INIT_ATTEMPTS => {
                last = Some(e);
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(INIT_BACKOFF_CAP);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.expect("busy retry exhausted"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pos {
    pub partition: u32,
    pub seq: u64,
}

impl Pos {
    pub fn local(seq: u64) -> Self {
        Pos {
            partition: LOCAL_PARTITION,
            seq,
        }
    }
}

#[derive(Debug)]
pub enum TrailError {
    EmptyBatch,
    InvalidLimit { max: usize },
    UnsupportedPartition { partition: u32 },
    Trimmed { resume_at: Pos },
    Corrupt { detail: String },
    SeqOutOfRange { seq: u64 },
    Storage(anyhow::Error),
}

impl std::fmt::Display for TrailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrailError::EmptyBatch => write!(f, "empty append batch"),
            TrailError::InvalidLimit { max } => {
                write!(f, "invalid read limit {max} (must be 1..={MAX_READ})")
            }
            TrailError::UnsupportedPartition { partition } => {
                write!(
                    f,
                    "unsupported partition {partition} (only {LOCAL_PARTITION})"
                )
            }
            TrailError::Trimmed { resume_at } => {
                write!(f, "trimmed: resume at seq {}", resume_at.seq)
            }
            TrailError::Corrupt { detail } => write!(f, "corrupt log row: {detail}"),
            TrailError::SeqOutOfRange { seq } => {
                write!(
                    f,
                    "sequence {seq} exceeds SQLite INTEGER ceiling ({SEQ_MAX})"
                )
            }
            TrailError::Storage(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for TrailError {}

impl From<rusqlite::Error> for TrailError {
    fn from(e: rusqlite::Error) -> Self {
        TrailError::Storage(anyhow::Error::new(e))
    }
}

pub trait TrailLog: Send + Sync {
    /// Durable batch append. Returns the last assigned Pos. Empty batch is an
    /// error and does not consume a sequence.
    fn append(&self, batch: &[Envelope]) -> Result<Pos, TrailError>;
    fn head(&self) -> Result<Vec<Pos>, TrailError>;
    /// Inclusive `from`, bounded `max`. `Trimmed` if `from` is at or below the
    /// retention floor.
    fn read(&self, from: Pos, max: usize) -> Result<Vec<(Pos, Envelope)>, TrailError>;
    fn seek_ts(&self, partition: u32, ts: &str) -> Result<Pos, TrailError>;
    /// Prefix retention by envelope.ts. Returns the new floor Pos.
    fn trim_before(&self, ts: &str) -> Result<Pos, TrailError>;
}

pub struct SqliteLog {
    conn: Mutex<Connection>,
}

impl SqliteLog {
    pub fn open_path(path: &Path) -> anyhow::Result<SqliteLog> {
        retry_busy(|| Self::open_path_once(path))
    }

    fn open_path_once(path: &Path) -> anyhow::Result<SqliteLog> {
        let conn =
            Connection::open(path).with_context(|| format!("cannot open {}", path.display()))?;
        // Keep an explicit short init timeout. Default is not 0ms:
        // rusqlite 0.32.1 Connection::open calls sqlite3_busy_timeout(db, 5000)
        // (inner_connection.rs:119). WAL conversion can still return BUSY
        // without invoking that handler; retry_busy wraps this attempt.
        conn.busy_timeout(INIT_BUSY_TIMEOUT)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS events (
                seq      INTEGER PRIMARY KEY,
                id       TEXT NOT NULL,
                ts       TEXT NOT NULL,
                subject  TEXT NOT NULL,
                envelope TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS events_id ON events(id);
            CREATE INDEX IF NOT EXISTS events_ts ON events(ts);
            CREATE TABLE IF NOT EXISTS log_state (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );",
        )?;
        conn.busy_timeout(WRITER_BUSY_TIMEOUT)?;
        let log = SqliteLog {
            conn: Mutex::new(conn),
        };
        log.seed_state()?;
        Ok(log)
    }

    pub fn open(paths: &Paths) -> anyhow::Result<SqliteLog> {
        paths.ensure()?;
        let log = Self::open_path(&paths.home.join("pher.db"))?;
        log.migrate_events_jsonl(paths)?;
        Ok(log)
    }

    /// Head is append-time metadata. Reopen never recomputes it from
    /// `MAX(events.seq)` (that is NULL after a full trim). A missing head row
    /// is a new log (0) or, once only, leftover pre-A1 event rows. One
    /// IMMEDIATE transaction so concurrent first opens cannot clobber a
    /// persisted head.
    fn seed_state(&self) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().expect("sqlite log mutex");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT OR IGNORE INTO log_state (key, value) VALUES ('head', 0)",
            [],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO log_state (key, value) VALUES ('floor', 0)",
            [],
        )?;
        let head = Self::get_state(&tx, "head")?;
        if head == 0 {
            let max: Option<i64> = tx.query_row("SELECT MAX(seq) FROM events", [], |r| r.get(0))?;
            if let Some(m) = max {
                if m > 0 {
                    let adopted = decode_seq(m)?;
                    tx.execute(
                        "UPDATE log_state SET value = ?1 WHERE key = 'head' AND value = 0",
                        params![m],
                    )?;
                    let _ = adopted;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Import `events.jsonl`. Head becomes the max `seq` field in the file
    /// (the historical assign), not `MAX(seq)` of whatever rows remain later.
    fn migrate_events_jsonl(&self, paths: &Paths) -> anyhow::Result<()> {
        let path = paths.events();
        if !path.exists() {
            return Ok(());
        }
        let text = std::fs::read_to_string(&path)?;
        let count = text.lines().count();
        let mut imported_head: u64 = 0;
        {
            let mut conn = self.conn.lock().expect("sqlite log mutex");
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let mut head = Self::get_state(&tx, "head")?;
            for line in text.lines() {
                let Ok(v) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                let (Some(seq), Some(event)) =
                    (v.get("seq").and_then(|s| s.as_u64()), v.get("event"))
                else {
                    continue;
                };
                let ts = event.get("ts").and_then(|t| t.as_str()).unwrap_or("");
                let id = event.get("id").and_then(|t| t.as_str()).unwrap_or("");
                let subject = event.get("subject").and_then(|t| t.as_str()).unwrap_or("");
                tx.execute(
                    "INSERT OR IGNORE INTO events (seq, id, ts, subject, envelope) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![encode_seq(seq)?, id, ts, subject, event.to_string()],
                )?;
                if seq > imported_head {
                    imported_head = seq;
                }
            }
            if imported_head > head {
                if imported_head > SEQ_MAX {
                    return Err(TrailError::SeqOutOfRange { seq: imported_head }.into());
                }
                head = imported_head;
                tx.execute(
                    "UPDATE log_state SET value = ?1 WHERE key = 'head' AND value < ?1",
                    params![encode_seq(head)?],
                )?;
            }
            tx.commit()?;
        }
        let backup = path.with_extension("jsonl.migrated");
        std::fs::rename(&path, &backup)?;
        eprintln!(
            "pherd: migrated {count} lines from {} into pher.db (original kept as {})",
            path.display(),
            backup.display()
        );
        Ok(())
    }

    fn require_p0(partition: u32) -> Result<(), TrailError> {
        if partition != LOCAL_PARTITION {
            return Err(TrailError::UnsupportedPartition { partition });
        }
        Ok(())
    }

    fn get_state(conn: &Connection, key: &str) -> Result<u64, TrailError> {
        let v: i64 = conn.query_row(
            "SELECT value FROM log_state WHERE key = ?1",
            params![key],
            |r| r.get(0),
        )?;
        decode_seq(v)
    }

    #[cfg(test)]
    pub fn floor(&self) -> Result<u64, TrailError> {
        let conn = self.conn.lock().expect("sqlite log mutex");
        Self::get_state(&conn, "floor")
    }

    pub fn head_seq(&self) -> Result<u64, TrailError> {
        let conn = self.conn.lock().expect("sqlite log mutex");
        Self::get_state(&conn, "head")
    }

    /// 2 = FULL. Used to verify writer durability config.
    #[cfg(test)]
    pub fn synchronous_pragma(&self) -> Result<i64, TrailError> {
        let conn = self.conn.lock().expect("sqlite log mutex");
        Ok(conn.query_row("PRAGMA synchronous", [], |r| r.get(0))?)
    }
}

impl TrailLog for SqliteLog {
    fn append(&self, batch: &[Envelope]) -> Result<Pos, TrailError> {
        if batch.is_empty() {
            return Err(TrailError::EmptyBatch);
        }
        let mut conn = self.conn.lock().expect("sqlite log mutex");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = Self::get_state(&tx, "head")?;
        let n = batch.len() as u64;
        let last = match head.checked_add(n) {
            Some(v) if v <= SEQ_MAX => v,
            Some(v) => return Err(TrailError::SeqOutOfRange { seq: v }),
            None => return Err(TrailError::SeqOutOfRange { seq: u64::MAX }),
        };
        let first = head + 1;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO events (seq, id, ts, subject, envelope) VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for (i, event) in batch.iter().enumerate() {
                let seq = first + i as u64;
                let envelope = serde_json::to_string(event)
                    .map_err(|e| TrailError::Storage(anyhow::Error::new(e)))?;
                stmt.execute(params![
                    encode_seq(seq)?,
                    event.id,
                    event.ts,
                    event.subject,
                    envelope
                ])?;
            }
        }
        tx.execute(
            "UPDATE log_state SET value = ?1 WHERE key = 'head'",
            params![encode_seq(last)?],
        )?;
        tx.commit()?;
        Ok(Pos::local(last))
    }

    fn head(&self) -> Result<Vec<Pos>, TrailError> {
        Ok(vec![Pos::local(self.head_seq()?)])
    }

    fn read(&self, from: Pos, max: usize) -> Result<Vec<(Pos, Envelope)>, TrailError> {
        Self::require_p0(from.partition)?;
        if max == 0 || max > MAX_READ {
            return Err(TrailError::InvalidLimit { max });
        }
        if from.seq > SEQ_MAX {
            return Ok(Vec::new());
        }
        let mut conn = self.conn.lock().expect("sqlite log mutex");
        let tx = conn.transaction()?;
        let floor = Self::get_state(&tx, "floor")?;
        let from_seq = from.seq.max(1);
        if floor > 0 && from_seq <= floor {
            let err = TrailError::Trimmed {
                resume_at: Pos::local(floor.saturating_add(1).min(SEQ_MAX)),
            };
            tx.commit()?;
            return Err(err);
        }
        let mut stmt = tx.prepare(
            "SELECT seq, envelope FROM events WHERE seq >= ?1 ORDER BY seq ASC LIMIT ?2",
        )?;
        let mapped = stmt.query_map(params![encode_seq(from_seq)?, max as i64], |r| {
            let seq_i: i64 = r.get(0)?;
            let text: String = r.get(1)?;
            Ok((seq_i, text))
        })?;
        let mut rows = Vec::new();
        for item in mapped {
            let (seq_i, text) = item?;
            let seq = decode_seq(seq_i)?;
            let event =
                serde_json::from_str::<Envelope>(&text).map_err(|e| TrailError::Corrupt {
                    detail: format!("seq {seq}: {e}"),
                })?;
            rows.push((Pos::local(seq), event));
        }
        drop(stmt);
        tx.commit()?;
        Ok(rows)
    }

    fn seek_ts(&self, partition: u32, ts: &str) -> Result<Pos, TrailError> {
        Self::require_p0(partition)?;
        let mut conn = self.conn.lock().expect("sqlite log mutex");
        let tx = conn.transaction()?;
        let floor = Self::get_state(&tx, "floor")?;
        let head = Self::get_state(&tx, "head")?;
        let found: Option<i64> = tx
            .query_row(
                "SELECT seq FROM events WHERE seq > ?1 AND ts >= ?2 ORDER BY seq ASC LIMIT 1",
                params![encode_seq(floor)?, ts],
                |r| r.get(0),
            )
            .optional()?;
        tx.commit()?;
        match found {
            Some(s) => Ok(Pos::local(decode_seq(s)?)),
            None => {
                let next = head.saturating_add(1);
                if next > SEQ_MAX {
                    Ok(Pos::local(SEQ_MAX))
                } else {
                    Ok(Pos::local(next))
                }
            }
        }
    }

    fn trim_before(&self, ts: &str) -> Result<Pos, TrailError> {
        let mut conn = self.conn.lock().expect("sqlite log mutex");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let floor = Self::get_state(&tx, "floor")?;
        let head = Self::get_state(&tx, "head")?;
        let first_kept: Option<i64> = tx
            .query_row(
                "SELECT seq FROM events WHERE seq > ?1 AND ts >= ?2 ORDER BY seq ASC LIMIT 1",
                params![encode_seq(floor)?, ts],
                |r| r.get(0),
            )
            .optional()?;
        let new_floor = match first_kept {
            Some(s) => decode_seq(s)?.saturating_sub(1).max(floor),
            None => head,
        };
        tx.execute(
            "DELETE FROM events WHERE seq > ?1 AND seq <= ?2",
            params![encode_seq(floor)?, encode_seq(new_floor)?],
        )?;
        tx.execute(
            "UPDATE log_state SET value = ?1 WHERE key = 'floor'",
            params![encode_seq(new_floor)?],
        )?;
        tx.commit()?;
        Ok(Pos::local(new_floor))
    }
}

/// Walk the log from `from_seq` (inclusive) through `through` (inclusive, if
/// set), one `LOG_PAGE` at a time. `Trimmed` and other read errors propagate.
/// `visit` returning false stops the walk.
pub fn for_each_page(
    log: &SqliteLog,
    mut from_seq: u64,
    through: Option<u64>,
    mut visit: impl FnMut(u64, Envelope) -> bool,
) -> Result<(), TrailError> {
    loop {
        if let Some(end) = through {
            if from_seq > end {
                return Ok(());
            }
        }
        match log.read(Pos::local(from_seq.max(1)), LOG_PAGE) {
            Ok(page) => {
                if page.is_empty() {
                    return Ok(());
                }
                for (pos, event) in page {
                    if let Some(end) = through {
                        if pos.seq > end {
                            return Ok(());
                        }
                    }
                    let next = pos.seq.saturating_add(1);
                    if !visit(pos.seq, event) {
                        return Ok(());
                    }
                    from_seq = next;
                }
            }
            Err(e) => return Err(e),
        }
    }
}

fn decode_seq(v: i64) -> Result<u64, TrailError> {
    if v < 0 {
        return Err(TrailError::Corrupt {
            detail: format!("negative sequence {v}"),
        });
    }
    Ok(v as u64)
}

fn encode_seq(v: u64) -> Result<i64, TrailError> {
    if v > SEQ_MAX {
        return Err(TrailError::SeqOutOfRange { seq: v });
    }
    Ok(v as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn tmp(name: &str) -> (SqliteLog, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "pher-traillog-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pher.db");
        (SqliteLog::open_path(&path).unwrap(), dir)
    }

    fn ev(id: &str, ts: &str, subject: &str) -> Envelope {
        Envelope {
            id: id.into(),
            ts: ts.into(),
            node: "n".into(),
            source: "s".into(),
            event_type: subject.into(),
            subject: subject.into(),
            correlation: None,
            payload: serde_json::json!({ "id": id }),
            ttl_class: None,
            hops: None,
        }
    }

    #[test]
    fn empty_batch_does_not_advance_head() {
        let (log, dir) = tmp("empty");
        assert!(matches!(log.append(&[]), Err(TrailError::EmptyBatch)));
        assert_eq!(log.head_seq().unwrap(), 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rollback_aborted_batch_leaves_head_and_rows() {
        let (log, dir) = tmp("rollback");
        log.append(&[ev("PH.a", "2026-08-01T00:00:00Z", "a")])
            .unwrap();
        {
            let conn = log.conn.lock().unwrap();
            conn.execute_batch(
                "CREATE TRIGGER fail_second BEFORE INSERT ON events
                 WHEN (SELECT COUNT(*) FROM events) >= 2
                 BEGIN
                   SELECT RAISE(ABORT, 'injected');
                 END;",
            )
            .unwrap();
        }
        let err = log
            .append(&[
                ev("PH.b", "2026-08-01T00:00:01Z", "b"),
                ev("PH.c", "2026-08-01T00:00:02Z", "c"),
            ])
            .unwrap_err();
        assert!(matches!(err, TrailError::Storage(_)), "{err}");
        assert_eq!(log.head_seq().unwrap(), 1);
        let page = log.read(Pos::local(1), 10).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].1.id, "PH.a");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn two_writers_assign_contiguous_unique_seqs() {
        let dir = std::env::temp_dir().join(format!(
            "pher-traillog-writers-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pher.db");
        let log_a = Arc::new(SqliteLog::open_path(&path).unwrap());
        let log_b = Arc::new(SqliteLog::open_path(&path).unwrap());
        let mut joins = Vec::new();
        for (t, log) in [(0u8, log_a.clone()), (1, log_b.clone())] {
            joins.push(std::thread::spawn(move || {
                let mut seqs = Vec::new();
                for i in 0..50 {
                    let e = ev(&format!("PH.{t}.{i}"), "2026-08-01T00:00:00Z", "w");
                    let pos = log.append(&[e]).unwrap();
                    seqs.push(pos.seq);
                }
                seqs
            }));
        }
        let mut all: Vec<u64> = joins.into_iter().flat_map(|j| j.join().unwrap()).collect();
        all.sort();
        all.dedup();
        assert_eq!(all, (1..=100).collect::<Vec<_>>(), "unique contiguous seqs");
        assert_eq!(log_a.head_seq().unwrap(), 100);
        assert_eq!(log_b.head_seq().unwrap(), 100);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn full_trim_then_restart_does_not_reuse_seq() {
        let dir = std::env::temp_dir().join(format!(
            "pher-traillog-restart-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pher.db");
        {
            let log = SqliteLog::open_path(&path).unwrap();
            for i in 1..=5 {
                log.append(&[ev(&format!("PH.{i}"), "2020-01-01T00:00:00Z", "old")])
                    .unwrap();
            }
            assert_eq!(log.head_seq().unwrap(), 5);
            let floor = log.trim_before("2026-01-01T00:00:00Z").unwrap();
            assert_eq!(floor.seq, 5);
            assert!(log.read(Pos::local(1), 10).is_err());
            assert_eq!(log.head_seq().unwrap(), 5);
            let max: Option<i64> = log
                .conn
                .lock()
                .unwrap()
                .query_row("SELECT MAX(seq) FROM events", [], |r| r.get(0))
                .unwrap();
            assert!(
                max.is_none(),
                "MAX(seq) is gone after trim; head must not follow it"
            );
        }
        {
            let log = SqliteLog::open_path(&path).unwrap();
            assert_eq!(log.head_seq().unwrap(), 5);
            let pos = log
                .append(&[ev("PH.new", "2026-09-01T00:00:00Z", "new")])
                .unwrap();
            assert_eq!(pos.seq, 6);
            let page = log.read(Pos::local(6), 10).unwrap();
            assert_eq!(page.len(), 1);
            assert_eq!(page[0].1.id, "PH.new");
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn wiped_event_rows_do_not_reset_persisted_head() {
        let dir = std::env::temp_dir().join(format!(
            "pher-traillog-wipe-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pher.db");
        {
            let log = SqliteLog::open_path(&path).unwrap();
            log.append(&[ev("PH.1", "2026-08-01T00:00:00Z", "a")])
                .unwrap();
            log.append(&[ev("PH.2", "2026-08-01T00:00:01Z", "a")])
                .unwrap();
            assert_eq!(log.head_seq().unwrap(), 2);
            log.conn
                .lock()
                .unwrap()
                .execute("DELETE FROM events", [])
                .unwrap();
        }
        let log = SqliteLog::open_path(&path).unwrap();
        assert_eq!(
            log.head_seq().unwrap(),
            2,
            "reopen must not treat MAX(seq) NULL as head 0"
        );
        let pos = log
            .append(&[ev("PH.3", "2026-08-01T00:00:02Z", "a")])
            .unwrap();
        assert_eq!(pos.seq, 3);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn bounded_read_rejects_zero_and_over_max_and_pages() {
        let (log, dir) = tmp("bounds");
        for i in 1..=5 {
            log.append(&[ev(&format!("PH.{i}"), "2026-08-01T00:00:00Z", "x")])
                .unwrap();
        }
        assert!(matches!(
            log.read(Pos::local(1), 0),
            Err(TrailError::InvalidLimit { max: 0 })
        ));
        assert!(matches!(
            log.read(Pos::local(1), MAX_READ + 1),
            Err(TrailError::InvalidLimit { .. })
        ));
        assert!(matches!(
            log.read(
                Pos {
                    partition: 1,
                    seq: 1
                },
                8
            ),
            Err(TrailError::UnsupportedPartition { partition: 1 })
        ));
        let p1 = log.read(Pos::local(1), 2).unwrap();
        assert_eq!(
            p1.iter().map(|(p, _)| p.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let p2 = log.read(Pos::local(3), 2).unwrap();
        assert_eq!(
            p2.iter().map(|(p, _)| p.seq).collect::<Vec<_>>(),
            vec![3, 4]
        );
        let p3 = log.read(Pos::local(5), 2).unwrap();
        assert_eq!(p3.len(), 1);
        assert!(log.read(Pos::local(6), 2).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn out_of_order_ts_trim_is_prefix_only_and_seek_is_seq_order() {
        let (log, dir) = tmp("ooo");
        // seq 1 recent, seq 2 forwarded with an older origin ts.
        log.append(&[ev("PH.1", "2026-09-01T00:00:00Z", "local")])
            .unwrap();
        log.append(&[ev("PH.2", "2020-01-01T00:00:00Z", "fwd")])
            .unwrap();
        let floor = log.trim_before("2026-06-01T00:00:00Z").unwrap();
        assert_eq!(floor.seq, 0, "seq 1 is kept; must not jump floor over it");
        let page = log.read(Pos::local(1), 10).unwrap();
        assert_eq!(page.len(), 2, "old-ts seq 2 sits after a kept event");
        let seek = log.seek_ts(0, "2026-06-01T00:00:00Z").unwrap();
        assert_eq!(seek.seq, 1, "first seq whose envelope.ts >= cutoff");

        // Prefix that *is* expired: old then new.
        let (log2, dir2) = tmp("ooo-prefix");
        log2.append(&[ev("PH.a", "2020-01-01T00:00:00Z", "old")])
            .unwrap();
        log2.append(&[ev("PH.b", "2026-09-01T00:00:00Z", "new")])
            .unwrap();
        let floor = log2.trim_before("2026-06-01T00:00:00Z").unwrap();
        assert_eq!(floor.seq, 1);
        match log2.read(Pos::local(1), 10) {
            Err(TrailError::Trimmed { resume_at }) => assert_eq!(resume_at.seq, 2),
            other => panic!("expected Trimmed, got {other:?}"),
        }
        let kept = log2.read(Pos::local(2), 10).unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].1.id, "PH.b");
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(dir2);
    }

    #[test]
    fn seek_ts_rejects_other_partitions() {
        let (log, dir) = tmp("part");
        assert!(matches!(
            log.seek_ts(2, "2026-01-01T00:00:00Z"),
            Err(TrailError::UnsupportedPartition { partition: 2 })
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn writer_uses_synchronous_full() {
        let (log, dir) = tmp("sync");
        assert_eq!(log.synchronous_pragma().unwrap(), 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn corrupt_json_is_an_error_not_eof() {
        let (log, dir) = tmp("corrupt");
        log.append(&[ev("PH.1", "2026-08-01T00:00:00Z", "a")])
            .unwrap();
        log.conn
            .lock()
            .unwrap()
            .execute("UPDATE events SET envelope = 'not-json' WHERE seq = 1", [])
            .unwrap();
        match log.read(Pos::local(1), 10) {
            Err(TrailError::Corrupt { detail }) => {
                assert!(detail.contains("seq 1"), "{detail}");
            }
            other => panic!("expected Corrupt, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn negative_head_is_corrupt() {
        let (log, dir) = tmp("neg");
        log.conn
            .lock()
            .unwrap()
            .execute("UPDATE log_state SET value = -1 WHERE key = 'head'", [])
            .unwrap();
        assert!(matches!(log.head_seq(), Err(TrailError::Corrupt { .. })));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn cursor_above_sqlite_ceiling_reads_empty() {
        let (log, dir) = tmp("ceil");
        log.append(&[ev("PH.1", "2026-08-01T00:00:00Z", "a")])
            .unwrap();
        assert!(log.read(Pos::local(SEQ_MAX + 1), 8).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    fn unique_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pher-traillog-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn concurrent_open_heads(path: &std::path::Path, n: usize) -> Vec<u64> {
        let hs: Vec<_> = (0..n)
            .map(|_| {
                let p = path.to_path_buf();
                std::thread::spawn(move || {
                    SqliteLog::open_path(&p).and_then(|l| l.head_seq().map_err(Into::into))
                })
            })
            .collect();
        hs.into_iter()
            .map(|h| h.join().expect("open thread").expect("open_path"))
            .collect()
    }

    fn plant_pre_a1_head(path: &std::path::Path, seq: i64) {
        let bootstrap = Connection::open(path).unwrap();
        bootstrap
            .execute_batch(
                "CREATE TABLE events (
                    seq INTEGER PRIMARY KEY,
                    id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    envelope TEXT NOT NULL
                );",
            )
            .unwrap();
        bootstrap
            .execute(
                "INSERT INTO events (seq, id, ts, subject, envelope) VALUES (?1, 'PH.7', '2026-01-01T00:00:00Z', 'x', '{}')",
                params![seq],
            )
            .unwrap();
    }

    #[test]
    fn concurrent_first_opens_do_not_lower_head() {
        for i in 0..24 {
            let dir = unique_dir(&format!("firstopen-{i}"));
            let path = dir.join("pher.db");
            plant_pre_a1_head(&path, 7);
            let heads = concurrent_open_heads(&path, 2);
            assert!(
                heads.iter().all(|&h| h == 7),
                "iter {i}: concurrent first opens must adopt head 7, got {heads:?}"
            );
            let log = SqliteLog::open_path(&path).unwrap();
            assert_eq!(log.head_seq().unwrap(), 7);
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn repeated_fresh_path_concurrent_opens_initialize() {
        for i in 0..24 {
            let dir = unique_dir(&format!("freshopen-{i}"));
            let path = dir.join("pher.db");
            let heads = concurrent_open_heads(&path, 4);
            assert!(
                heads.iter().all(|&h| h == 0),
                "iter {i}: fresh concurrent opens must seed head 0, got {heads:?}"
            );
            let log = SqliteLog::open_path(&path).unwrap();
            assert_eq!(log.head_seq().unwrap(), 0);
            let pos = log
                .append(&[ev("PH.1", "2026-08-01T00:00:00Z", "a")])
                .unwrap();
            assert_eq!(pos.seq, 1);
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn trim_on_other_connection_does_not_turn_prefix_into_empty_page() {
        let dir = std::env::temp_dir().join(format!(
            "pher-traillog-snap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pher.db");
        let writer = SqliteLog::open_path(&path).unwrap();
        for i in 1..=5 {
            writer
                .append(&[ev(&format!("PH.{i}"), "2020-01-01T00:00:00Z", "old")])
                .unwrap();
        }
        let reader = SqliteLog::open_path(&path).unwrap();
        writer.trim_before("2026-01-01T00:00:00Z").unwrap();
        match reader.read(Pos::local(1), 10) {
            Err(TrailError::Trimmed { resume_at }) => assert_eq!(resume_at.seq, 6),
            Ok(page) => panic!("expected Trimmed after peer trim, got {page:?}"),
            Err(e) => panic!("unexpected {e}"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
