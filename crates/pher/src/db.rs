//! SQLite storage for the growing logs: events, deliveries, judge verdicts,
//! the semantic vector window, and the cross-trail dedup window. One WAL-mode
//! database (`pher.db`), so kill -9 keeps every committed transaction and
//! reads are indexed instead of whole-file scans — the JSONL logs stopped
//! scaling the moment a real tap (hive, hundreds of events/min) landed.
//!
//! Small operator state (subs, timers, cursors, grants, bridges, outbox,
//! conditions) stays as human-readable JSON files: tiny, atomic via
//! temp+rename, and greppable at 3am.
//!
//! Existing JSONL logs migrate automatically on first open; the originals
//! are renamed to `*.migrated` rather than deleted.

use anyhow::Context;
use pher_core::Envelope;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;

use crate::store::Paths;

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn open(paths: &Paths) -> anyhow::Result<Db> {
        paths.ensure()?;
        let path = paths.home.join("pher.db");
        let conn =
            Connection::open(&path).with_context(|| format!("cannot open {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
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
            CREATE TABLE IF NOT EXISTS deliveries (
                delivery_id TEXT PRIMARY KEY,
                ts          TEXT NOT NULL,
                record      TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS deliveries_ts ON deliveries(ts);
            CREATE TABLE IF NOT EXISTS verdicts (
                key    TEXT PRIMARY KEY,
                ts     INTEGER NOT NULL,
                record TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS vectors (
                ts       INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                vec      BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS forwarded (
                id TEXT PRIMARY KEY
            );",
        )?;
        let mut db = Db { conn };
        db.migrate_jsonl(paths)?;
        Ok(db)
    }

    /// One-time import of the pre-SQLite JSONL logs, streaming, in one
    /// transaction per file. Renames each source to `<name>.migrated`.
    fn migrate_jsonl(&mut self, paths: &Paths) -> anyhow::Result<()> {
        type Import = fn(&Connection, &Value);
        let migrations: [(std::path::PathBuf, Import); 5] = [
            (paths.events(), |c, v| {
                if let (Some(seq), Some(event)) =
                    (v.get("seq").and_then(|s| s.as_u64()), v.get("event"))
                {
                    let ts = event.get("ts").and_then(|t| t.as_str()).unwrap_or("");
                    let id = event.get("id").and_then(|t| t.as_str()).unwrap_or("");
                    let subject = event.get("subject").and_then(|t| t.as_str()).unwrap_or("");
                    let _ = c.execute(
                        "INSERT OR IGNORE INTO events (seq, id, ts, subject, envelope) VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![seq, id, ts, subject, event.to_string()],
                    );
                }
            }),
            (paths.deliveries(), |c, v| {
                if let Some(id) = v.get("deliveryId").and_then(|d| d.as_str()) {
                    let ts = v.get("ts").and_then(|t| t.as_str()).unwrap_or("");
                    let _ = c.execute(
                        "INSERT OR IGNORE INTO deliveries (delivery_id, ts, record) VALUES (?1, ?2, ?3)",
                        params![id, ts, v.to_string()],
                    );
                }
            }),
            (paths.verdicts(), |c, v| {
                if let Some(key) = v.get("key").and_then(|k| k.as_str()) {
                    let ts = v.get("ts").and_then(|t| t.as_u64()).unwrap_or(0);
                    let _ = c.execute(
                        "INSERT OR REPLACE INTO verdicts (key, ts, record) VALUES (?1, ?2, ?3)",
                        params![key, ts, v.to_string()],
                    );
                }
            }),
            (paths.vectors(), |c, v| {
                if let (Some(ts), Some(vec)) = (v.get("ts").and_then(|t| t.as_u64()), v.get("vec"))
                {
                    if let Ok(vec) = serde_json::from_value::<Vec<f32>>(vec.clone()) {
                        let id = v.get("id").and_then(|i| i.as_str()).unwrap_or("");
                        let _ = c.execute(
                            "INSERT INTO vectors (ts, event_id, vec) VALUES (?1, ?2, ?3)",
                            params![ts, id, f32s_to_blob(&vec)],
                        );
                    }
                }
            }),
            (paths.forwarded(), |c, v| {
                if let Some(id) = v.get("id").and_then(|i| i.as_str()) {
                    let _ = c.execute(
                        "INSERT OR IGNORE INTO forwarded (id) VALUES (?1)",
                        params![id],
                    );
                }
            }),
        ];
        for (path, import) in migrations {
            if !path.exists() {
                continue;
            }
            let text = std::fs::read_to_string(&path)?;
            let count = text.lines().count();
            let tx = self.conn.transaction()?;
            for line in text.lines() {
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    import(&tx, &v);
                }
            }
            tx.commit()?;
            let backup = path.with_extension("jsonl.migrated");
            std::fs::rename(&path, &backup)?;
            eprintln!(
                "pherd: migrated {count} lines from {} into pher.db (original kept as {})",
                path.display(),
                backup.display()
            );
        }
        Ok(())
    }

    // -- events ---------------------------------------------------------------

    pub fn append_event(&self, seq: u64, event: &Envelope) -> anyhow::Result<()> {
        self.conn.execute(
            "INSERT INTO events (seq, id, ts, subject, envelope) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                seq,
                event.id,
                event.ts,
                event.subject,
                serde_json::to_string(event)?
            ],
        )?;
        Ok(())
    }

    pub fn max_seq(&self) -> anyhow::Result<Option<u64>> {
        Ok(self
            .conn
            .query_row("SELECT MAX(seq) FROM events", [], |r| {
                r.get::<_, Option<u64>>(0)
            })?)
    }

    /// Events with seq > after, ascending. `limit_last` keeps only the most
    /// recent N of the range (console feeds want context, not history).
    pub fn events_after(
        &self,
        after: Option<u64>,
        limit_last: Option<usize>,
    ) -> anyhow::Result<Vec<(u64, Envelope)>> {
        let mut rows = match limit_last {
            Some(n) => {
                let mut stmt = self.conn.prepare_cached(
                    "SELECT seq, envelope FROM (
                        SELECT seq, envelope FROM events WHERE seq > ?1 ORDER BY seq DESC LIMIT ?2
                     ) ORDER BY seq ASC",
                )?;
                let rows: Vec<_> = stmt
                    .query_map(params![after.unwrap_or(0), n as u64], row_to_event)?
                    .filter_map(Result::ok)
                    .collect();
                rows
            }
            None => {
                let mut stmt = self.conn.prepare_cached(
                    "SELECT seq, envelope FROM events WHERE seq > ?1 ORDER BY seq ASC",
                )?;
                let rows: Vec<_> = stmt
                    .query_map(params![after.unwrap_or(0)], row_to_event)?
                    .filter_map(Result::ok)
                    .collect();
                rows
            }
        };
        rows.retain(|(_, e)| !e.id.is_empty());
        Ok(rows)
    }

    /// Events at or after an RFC3339 timestamp (time-based `since` replay).
    pub fn events_since_ts(&self, ts: &str) -> anyhow::Result<Vec<(u64, Envelope)>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT seq, envelope FROM events WHERE ts >= ?1 ORDER BY seq ASC")?;
        let rows = stmt
            .query_map(params![ts], row_to_event)?
            .filter_map(Result::ok)
            .collect();
        Ok(rows)
    }

    pub fn event_by_id(&self, id: &str) -> anyhow::Result<Option<Envelope>> {
        let row: Option<String> = self
            .conn
            .query_row(
                "SELECT envelope FROM events WHERE id = ?1 ORDER BY seq DESC LIMIT 1",
                params![id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(row.and_then(|t| serde_json::from_str(&t).ok()))
    }

    // -- deliveries -----------------------------------------------------------

    pub fn append_delivery(
        &self,
        delivery_id: &str,
        ts: &str,
        record: &Value,
    ) -> anyhow::Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO deliveries (delivery_id, ts, record) VALUES (?1, ?2, ?3)",
            params![delivery_id, ts, record.to_string()],
        )?;
        Ok(())
    }

    pub fn delivery_by_id(&self, delivery_id: &str) -> anyhow::Result<Option<Value>> {
        let row: Option<String> = self
            .conn
            .query_row(
                "SELECT record FROM deliveries WHERE delivery_id = ?1",
                params![delivery_id],
                |r| r.get(0),
            )
            .optional()?;
        Ok(row.and_then(|t| serde_json::from_str(&t).ok()))
    }

    // -- verdicts ------------------------------------------------------------

    pub fn append_verdict(&self, key: &str, ts: u64, record: &Value) -> anyhow::Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO verdicts (key, ts, record) VALUES (?1, ?2, ?3)",
            params![key, ts, record.to_string()],
        )?;
        Ok(())
    }

    pub fn load_verdicts(&self) -> anyhow::Result<Vec<Value>> {
        let mut stmt = self.conn.prepare("SELECT record FROM verdicts")?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .filter_map(Result::ok)
            .filter_map(|t| serde_json::from_str(&t).ok())
            .collect();
        Ok(rows)
    }

    // -- semantic vector window ------------------------------------------------

    pub fn append_vector(&self, ts: u64, event_id: &str, vec: &[f32]) -> anyhow::Result<()> {
        self.conn.execute(
            "INSERT INTO vectors (ts, event_id, vec) VALUES (?1, ?2, ?3)",
            params![ts, event_id, f32s_to_blob(vec)],
        )?;
        Ok(())
    }

    /// The most recent `limit` vectors, oldest first (novelty window shape).
    pub fn load_vectors(&self, limit: usize) -> anyhow::Result<Vec<(u64, String, Vec<f32>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT ts, event_id, vec FROM (
                SELECT rowid, ts, event_id, vec FROM vectors ORDER BY rowid DESC LIMIT ?1
             ) ORDER BY rowid ASC",
        )?;
        let rows = stmt
            .query_map(params![limit as u64], |r| {
                Ok((
                    r.get::<_, u64>(0)?,
                    r.get::<_, String>(1)?,
                    blob_to_f32s(&r.get::<_, Vec<u8>>(2)?),
                ))
            })?
            .filter_map(Result::ok)
            .collect();
        Ok(rows)
    }

    // -- cross-trail dedup window --------------------------------------------------

    pub fn forwarded_insert(&self, id: &str) -> anyhow::Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO forwarded (id) VALUES (?1)",
            params![id],
        )?;
        Ok(())
    }

    pub fn load_forwarded(&self, limit: usize) -> anyhow::Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT id FROM (
                SELECT rowid, id FROM forwarded ORDER BY rowid DESC LIMIT ?1
             ) ORDER BY rowid ASC",
        )?;
        let rows = stmt
            .query_map(params![limit as u64], |r| r.get::<_, String>(0))?
            .filter_map(Result::ok)
            .collect();
        Ok(rows)
    }

    // -- GC (evaporation) ---------------------------------------------------------

    pub fn gc(
        &self,
        ts_cutoff: &str,
        unix_cutoff: u64,
        forwarded_keep: usize,
    ) -> anyhow::Result<()> {
        self.conn
            .execute("DELETE FROM events WHERE ts < ?1", params![ts_cutoff])?;
        self.conn
            .execute("DELETE FROM deliveries WHERE ts < ?1", params![ts_cutoff])?;
        self.conn
            .execute("DELETE FROM verdicts WHERE ts < ?1", params![unix_cutoff])?;
        self.conn
            .execute("DELETE FROM vectors WHERE ts < ?1", params![unix_cutoff])?;
        self.conn.execute(
            "DELETE FROM forwarded WHERE rowid <= (SELECT COALESCE(MAX(rowid), 0) FROM forwarded) - ?1",
            params![forwarded_keep as u64],
        )?;
        Ok(())
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

fn f32s_to_blob(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn blob_to_f32s(b: &[u8]) -> Vec<f32> {
    b.chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

/// Migration keeps `.jsonl.migrated` backups; this is where a `pher doctor`
/// could offer to trash them once the operator trusts the db.
#[allow(dead_code)]
pub fn migrated_backup_paths(paths: &Paths) -> Vec<std::path::PathBuf> {
    [
        "events",
        "deliveries",
        "verdicts",
        "vectors",
        "forwarded_seen",
    ]
    .iter()
    .map(|n| paths.home.join(format!("{n}.jsonl.migrated")))
    .filter(|p| p.exists())
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp(name: &str) -> (Db, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("pher-db-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let paths = Paths { home: dir.clone() };
        (Db::open(&paths).unwrap(), dir)
    }

    fn ev(seq: u64, subject: &str) -> Envelope {
        Envelope {
            id: format!("PH.{seq}"),
            ts: format!("2026-08-12T00:00:{seq:02}.000Z"),
            node: "n".into(),
            source: "s".into(),
            event_type: subject.into(),
            subject: subject.into(),
            correlation: None,
            payload: serde_json::json!({"n": seq}),
            ttl_class: None,
            hops: None,
        }
    }

    #[test]
    fn events_roundtrip_ranges_and_gc() {
        let (db, dir) = open_tmp("events");
        for i in 1..=10u64 {
            db.append_event(i, &ev(i, "demo.tick")).unwrap();
        }
        assert_eq!(db.max_seq().unwrap(), Some(10));
        assert_eq!(db.events_after(Some(7), None).unwrap().len(), 3);
        let last2 = db.events_after(None, Some(2)).unwrap();
        assert_eq!(
            last2.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![9, 10]
        );
        assert_eq!(
            db.event_by_id("PH.5").unwrap().unwrap().payload["n"],
            serde_json::json!(5)
        );
        assert!(db.event_by_id("PH.nope").unwrap().is_none());
        assert_eq!(db.events_since_ts("2026-08-12T00:00:08").unwrap().len(), 3);
        db.gc("2026-08-12T00:00:06", 0, 10).unwrap();
        assert_eq!(db.events_after(None, None).unwrap().len(), 5);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn vectors_and_forwarded_windows() {
        let (db, dir) = open_tmp("vectors");
        for i in 0..5 {
            db.append_vector(100 + i, &format!("E{i}"), &[i as f32, 1.0])
                .unwrap();
            db.forwarded_insert(&format!("F{i}")).unwrap();
        }
        let vecs = db.load_vectors(3).unwrap();
        assert_eq!(vecs.len(), 3);
        assert_eq!(vecs[0].1, "E2"); // oldest of the kept tail
        assert_eq!(vecs[2].2, vec![4.0, 1.0]);
        db.forwarded_insert("F0").unwrap(); // dup ignored
        assert_eq!(db.load_forwarded(100).unwrap().len(), 5);
        db.gc("9999", 9999, 2).unwrap(); // keep last 2 forwarded
        assert_eq!(db.load_forwarded(100).unwrap(), vec!["F3", "F4"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn jsonl_migration_imports_and_renames() {
        let dir = std::env::temp_dir().join(format!("pher-mig-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let paths = Paths { home: dir.clone() };
        std::fs::write(
            paths.events(),
            r#"{"seq":1,"event":{"id":"PH.a","ts":"2026-08-12T00:00:01.000Z","node":"n","source":"s","type":"t","subject":"a.b","payload":{"x":1}}}
{"seq":2,"event":{"id":"PH.b","ts":"2026-08-12T00:00:02.000Z","node":"n","source":"s","type":"t","subject":"a.c","payload":{}}}
not json — torn line survives migration
"#,
        )
        .unwrap();
        std::fs::write(paths.forwarded(), "{\"id\":\"D1\"}\n{\"id\":\"D1\"}\n").unwrap();
        let db = Db::open(&paths).unwrap();
        assert_eq!(db.events_after(None, None).unwrap().len(), 2);
        assert_eq!(db.load_forwarded(10).unwrap(), vec!["D1"]);
        assert!(!paths.events().exists());
        assert!(paths.home.join("events.jsonl.migrated").exists());
        // Re-open: no double import.
        drop(db);
        let db = Db::open(&paths).unwrap();
        assert_eq!(db.events_after(None, None).unwrap().len(), 2);
        let _ = std::fs::remove_dir_all(dir);
    }
}
