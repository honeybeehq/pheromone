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
    pub fn open(paths: &Paths) -> anyhow::Result<SqliteLog> {
        paths.ensure()?;
        Self::open_path(&paths.home.join("pher.db"))
    }

    pub fn open_path(path: &Path) -> anyhow::Result<SqliteLog> {
        let conn =
            Connection::open(path).with_context(|| format!("cannot open {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.busy_timeout(Duration::from_secs(5))?;
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
        let log = SqliteLog {
            conn: Mutex::new(conn),
        };
        log.seed_state()?;
        Ok(log)
    }

    fn seed_state(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().expect("sqlite log mutex");
        let head: Option<i64> = conn
            .query_row("SELECT value FROM log_state WHERE key = 'head'", [], |r| {
                r.get(0)
            })
            .optional()?;
        if head.is_none() {
            let max: Option<i64> =
                conn.query_row("SELECT MAX(seq) FROM events", [], |r| r.get(0))?;
            conn.execute(
                "INSERT INTO log_state (key, value) VALUES ('head', ?1)",
                params![max.unwrap_or(0)],
            )?;
        }
        let floor: Option<i64> = conn
            .query_row("SELECT value FROM log_state WHERE key = 'floor'", [], |r| {
                r.get(0)
            })
            .optional()?;
        if floor.is_none() {
            conn.execute("INSERT INTO log_state (key, value) VALUES ('floor', 0)", [])?;
        }
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
        Ok(v as u64)
    }

    pub fn floor(&self) -> Result<u64, TrailError> {
        let conn = self.conn.lock().expect("sqlite log mutex");
        Self::get_state(&conn, "floor")
    }

    pub fn head_seq(&self) -> Result<u64, TrailError> {
        let conn = self.conn.lock().expect("sqlite log mutex");
        Self::get_state(&conn, "head")
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
        let first = head + 1;
        let last = head + batch.len() as u64;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO events (seq, id, ts, subject, envelope) VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for (i, event) in batch.iter().enumerate() {
                let seq = first + i as u64;
                let envelope = serde_json::to_string(event)
                    .map_err(|e| TrailError::Storage(anyhow::Error::new(e)))?;
                stmt.execute(params![seq, event.id, event.ts, event.subject, envelope])?;
            }
        }
        tx.execute(
            "UPDATE log_state SET value = ?1 WHERE key = 'head'",
            params![last as i64],
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
        let conn = self.conn.lock().expect("sqlite log mutex");
        let floor = Self::get_state(&conn, "floor")?;
        let from_seq = from.seq.max(1);
        if floor > 0 && from_seq <= floor {
            return Err(TrailError::Trimmed {
                resume_at: Pos::local(floor + 1),
            });
        }
        let mut stmt = conn.prepare(
            "SELECT seq, envelope FROM events WHERE seq >= ?1 ORDER BY seq ASC LIMIT ?2",
        )?;
        let rows: Result<Vec<_>, TrailError> = stmt
            .query_map(params![from_seq, max as u64], row_to_event)?
            .map(|r| {
                r.map(|(seq, event)| (Pos::local(seq), event))
                    .map_err(TrailError::from)
            })
            .collect();
        let mut rows = rows?;
        rows.retain(|(_, e)| !e.id.is_empty());
        Ok(rows)
    }

    fn seek_ts(&self, partition: u32, ts: &str) -> Result<Pos, TrailError> {
        Self::require_p0(partition)?;
        let conn = self.conn.lock().expect("sqlite log mutex");
        let floor = Self::get_state(&conn, "floor")?;
        let head = Self::get_state(&conn, "head")?;
        // Seq order, envelope.ts (events.ts). Not Comb Frame.at, not ORDER BY ts.
        let found: Option<i64> = conn
            .query_row(
                "SELECT seq FROM events WHERE seq > ?1 AND ts >= ?2 ORDER BY seq ASC LIMIT 1",
                params![floor as i64, ts],
                |r| r.get(0),
            )
            .optional()?;
        Ok(Pos::local(
            found.map(|s| s as u64).unwrap_or(head.saturating_add(1)),
        ))
    }

    fn trim_before(&self, ts: &str) -> Result<Pos, TrailError> {
        let mut conn = self.conn.lock().expect("sqlite log mutex");
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let floor = Self::get_state(&tx, "floor")?;
        let head = Self::get_state(&tx, "head")?;
        let rows: Vec<(u64, String)> = {
            let mut stmt =
                tx.prepare("SELECT seq, ts FROM events WHERE seq > ?1 ORDER BY seq ASC")?;
            let rows = stmt
                .query_map(params![floor as i64], |r| {
                    Ok((r.get::<_, i64>(0)? as u64, r.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let new_floor = match rows.iter().find(|(_, event_ts)| event_ts.as_str() >= ts) {
            Some((seq, _)) => seq.saturating_sub(1).max(floor),
            None => head,
        };
        tx.execute(
            "DELETE FROM events WHERE seq > ?1 AND seq <= ?2",
            params![floor as i64, new_floor as i64],
        )?;
        tx.execute(
            "UPDATE log_state SET value = ?1 WHERE key = 'floor'",
            params![new_floor as i64],
        )?;
        tx.commit()?;
        Ok(Pos::local(new_floor))
    }
}

/// Walk the log from `from_seq` (inclusive) through `through` (inclusive, if
/// set), one `LOG_PAGE` at a time. `Trimmed` skips to `resume_at`. `visit`
/// returning false stops the walk.
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
            Err(TrailError::Trimmed { resume_at }) => {
                from_seq = resume_at.seq;
            }
            Err(e) => return Err(e),
        }
    }
}

fn row_to_event(r: &rusqlite::Row) -> rusqlite::Result<(u64, Envelope)> {
    let seq: u64 = r.get(0)?;
    let text: String = r.get(1)?;
    let event = serde_json::from_str::<Envelope>(&text).unwrap_or_else(|_| Envelope {
        id: String::new(),
        ts: String::new(),
        node: String::new(),
        source: String::new(),
        event_type: String::new(),
        subject: String::new(),
        correlation: None,
        payload: Value::Null,
        ttl_class: None,
        hops: None,
    });
    Ok((seq, event))
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
}
