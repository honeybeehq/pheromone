//! Connectors: vendor extractors as DATA. A manifest (TOML) describes how a
//! SaaS vendor's events are fetched — auth header shape, poll endpoints with
//! cursors, subject mapping — and one generic engine executes any manifest.
//! The library of integrations is therefore a directory of manifests with
//! fixture tests, not a codebase. For vendors a manifest can't express,
//! `[exec]` supervises any process that emits envelope JSONL on stdout: a
//! plugin protocol, not a plugin API.
//!
//! Connectors answer "what are this vendor's EVENTS?", never "what are its
//! tables?" — this is an event extractor, not an ELT tool. First poll
//! establishes the watermark and emits nothing (live-from-now, like taps).
//!
//! Instances are daemon-native like bridges: persisted (tokens 0600),
//! supervised with backoff, cursor state persisted so restarts never
//! re-emit. Tokens are resolved by the CLI at add/apply time (literal /
//! env:VAR / cmd:...) — secret managers (hem, 1Password, pass) are addons
//! via cmd:, never a dependency.

use std::collections::HashMap;
use std::io::BufRead;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::daemon::State;
use crate::protocol::PartialEvent;

// -- manifests (the catalog) --------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub docs: String,
    #[serde(default)]
    pub auth: Option<AuthSpec>,
    /// Required instance params (name → human description), e.g. org = "…".
    #[serde(default)]
    pub params: HashMap<String, String>,
    #[serde(default, rename = "poll")]
    pub polls: Vec<PollSpec>,
    #[serde(default)]
    pub exec: Option<ExecSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSpec {
    /// Header name, e.g. "Authorization".
    pub header: String,
    /// Header value template; `{token}` is substituted.
    pub format: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollSpec {
    pub name: String,
    /// URL template; `{param}` substituted from instance params (+ token).
    pub url: String,
    /// e.g. "60s". Clamped to >= 5s at run time.
    pub interval: String,
    /// Where the record array lives in the response: "$" = body root,
    /// otherwise a dotted field path (e.g. "data").
    #[serde(default = "default_records")]
    pub records: String,
    /// Subject template over record fields, e.g. "sentry.issue.{level}".
    pub subject: String,
    /// Record field holding the vendor id (dedup key). Dotted path.
    pub id: String,
    /// Record field holding the event timestamp (envelope ts hint). Dotted.
    #[serde(default)]
    pub ts: Option<String>,
    /// Record field to watermark on (only records with field > stored
    /// watermark are emitted). Usually a timestamp; compared as strings for
    /// ISO-8601, numerically when both sides parse as numbers.
    #[serde(default)]
    pub watermark: Option<String>,
}

fn default_records() -> String {
    "$".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecSpec {
    /// Command + args; stdout must be envelope JSONL ({subject, payload, ...}).
    pub command: Vec<String>,
}

impl Manifest {
    pub fn parse(text: &str) -> Result<Manifest, String> {
        let m: Manifest = toml::from_str(text).map_err(|e| e.to_string())?;
        if m.polls.is_empty() && m.exec.is_none() {
            return Err(format!(
                "manifest '{}' has neither [[poll]] nor [exec]",
                m.name
            ));
        }
        for p in &m.polls {
            pher_core::Dur::parse(&p.interval)
                .map_err(|e| format!("poll '{}': bad interval: {e}", p.name))?;
        }
        Ok(m)
    }
}

/// Built-in catalog, embedded so `pher connect add sentry` works with zero
/// setup. User manifests in ~/.pheromone/connectors/*.toml override by name.
const BUILTINS: &[(&str, &str)] = &[
    ("sentry", include_str!("../connectors/sentry.toml")),
    ("github", include_str!("../connectors/github.toml")),
    ("stripe", include_str!("../connectors/stripe.toml")),
];

pub fn catalog(paths: &crate::store::Paths) -> Vec<(String, Manifest, &'static str)> {
    let mut out: Vec<(String, Manifest, &'static str)> = Vec::new();
    for (name, text) in BUILTINS {
        if let Ok(m) = Manifest::parse(text) {
            out.push((name.to_string(), m, "built-in"));
        }
    }
    if let Ok(dir) = std::fs::read_dir(paths.connectors_dir()) {
        for entry in dir.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            match Manifest::parse(&text) {
                Ok(m) => {
                    let name = m.name.clone();
                    out.retain(|(n, _, _)| n != &name); // local overrides built-in
                    out.push((name, m, "local"));
                }
                Err(e) => eprintln!("pherd: warning: bad manifest {}: {e}", path.display()),
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

pub fn find_manifest(paths: &crate::store::Paths, name: &str) -> Option<Manifest> {
    catalog(paths)
        .into_iter()
        .find(|(n, _, _)| n == name)
        .map(|(_, m, _)| m)
}

// -- instances ---------------------------------------------------------------

/// A configured connector: which manifest, whose token, which params.
/// Persisted in connectors.json (0600 — the token is in here).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorDef {
    pub name: String,
    /// Manifest name in the catalog.
    #[serde(rename = "use")]
    pub manifest: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub params: HashMap<String, String>,
}

/// Per-poll cursor state, persisted so restarts never re-emit.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PollState {
    #[serde(default)]
    pub watermark: String,
    /// Recently seen vendor ids (covers watermark ties and wm-less polls).
    #[serde(default)]
    pub recent: Vec<String>,
}

const RECENT_IDS: usize = 500;

// -- field paths & templates ---------------------------------------------------

/// Dotted-path lookup into a record ("a.b.c").
pub fn field<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = record;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

fn field_str(record: &Value, path: &str) -> Option<String> {
    field(record, path).map(|v| match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    })
}

/// Substitute `{key}` placeholders from the map; unknown keys become "unknown".
pub fn template(text: &str, vars: &dyn Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let Some(end) = rest[start..].find('}') else {
            out.push_str(&rest[start..]);
            return out;
        };
        let key = &rest[start + 1..start + end];
        out.push_str(&vars(key).unwrap_or_else(|| "unknown".to_string()));
        rest = &rest[start + end + 1..];
    }
    out.push_str(rest);
    out
}

/// Subject-safe token (mirrors the hive tap's sanitizer).
fn sanitize_token(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c.to_ascii_lowercase()
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
}

/// Watermark comparison: numeric when both parse, else lexicographic
/// (correct for ISO-8601 timestamps).
fn wm_newer(candidate: &str, stored: &str) -> bool {
    if stored.is_empty() {
        return true;
    }
    match (candidate.parse::<f64>(), stored.parse::<f64>()) {
        (Ok(c), Ok(s)) => c > s,
        _ => candidate > stored,
    }
}

/// Pure poll-cycle core: which records are NEW given the state, and the
/// updated state. Records are processed oldest-first regardless of API
/// order, so watermark advancement is monotonic.
pub fn filter_new(
    records: &[Value],
    spec: &PollSpec,
    state: &PollState,
) -> (Vec<Value>, PollState) {
    let mut new_state = state.clone();
    let mut fresh: Vec<(String, Value)> = Vec::new();
    for r in records {
        let Some(id) = field_str(r, &spec.id) else {
            continue; // no id, no dedup, no admission
        };
        if state.recent.contains(&id) {
            continue;
        }
        if let Some(wm_field) = &spec.watermark {
            let wm = field_str(r, wm_field).unwrap_or_default();
            // Admit strictly-newer, AND watermark ties (two events in the
            // same second) — the recent-ids window dedups tie repeats.
            let admissible = state.watermark.is_empty()
                || wm_newer(&wm, &state.watermark)
                || wm == state.watermark;
            if !admissible {
                continue;
            }
            fresh.push((wm, r.clone()));
        } else {
            fresh.push((String::new(), r.clone()));
        }
    }
    fresh.sort_by(|a, b| a.0.cmp(&b.0)); // oldest first
    for (wm, r) in &fresh {
        if wm_newer(wm, &new_state.watermark) {
            new_state.watermark = wm.clone();
        }
        if let Some(id) = field_str(r, &spec.id) {
            new_state.recent.push(id);
        }
    }
    let excess = new_state.recent.len().saturating_sub(RECENT_IDS);
    if excess > 0 {
        new_state.recent.drain(..excess);
    }
    (fresh.into_iter().map(|(_, r)| r).collect(), new_state)
}

// -- the engine ---------------------------------------------------------------

pub(crate) fn spawn(state: Arc<Mutex<State>>, def: ConnectorDef, cancel: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let paths = state.lock().unwrap().paths_clone();
        let Some(manifest) = find_manifest(&paths, &def.manifest) else {
            eprintln!(
                "pherd connector '{}': manifest '{}' not in catalog — worker not started",
                def.name, def.manifest
            );
            return;
        };
        if let Some(exec) = &manifest.exec {
            run_exec(&state, &def, exec, &cancel);
        } else {
            run_polls(&state, &def, &manifest, &cancel);
        }
    });
}

fn run_polls(
    state: &Arc<Mutex<State>>,
    def: &ConnectorDef,
    manifest: &Manifest,
    cancel: &Arc<AtomicBool>,
) {
    let vars = |key: &str| -> Option<String> {
        if key == "token" {
            Some(def.token.clone())
        } else {
            def.params.get(key).cloned()
        }
    };
    let mut due: HashMap<String, u64> = HashMap::new(); // poll name -> unix next
    loop {
        if cancel.load(Ordering::Relaxed) {
            return;
        }
        let now = now_unix();
        for spec in &manifest.polls {
            if *due.get(&spec.name).unwrap_or(&0) > now {
                continue;
            }
            let interval = pher_core::Dur::parse(&spec.interval)
                .map(|d| d.secs().max(5))
                .unwrap_or(60);
            due.insert(spec.name.clone(), now + interval);

            let url = template(&spec.url, &vars);
            let mut req = ureq::get(&url)
                .timeout(Duration::from_secs(20))
                .set("accept", "application/json")
                .set("user-agent", "pher-connector");
            if let Some(auth) = &manifest.auth {
                req = req.set(&auth.header, &template(&auth.format, &vars));
            }
            let body = match req.call() {
                Ok(resp) => resp.into_string().unwrap_or_default(),
                Err(e) => {
                    eprintln!("pherd connector '{}' poll '{}': {e}", def.name, spec.name);
                    continue;
                }
            };
            let Ok(parsed) = serde_json::from_str::<Value>(&body) else {
                eprintln!(
                    "pherd connector '{}' poll '{}': response is not JSON",
                    def.name, spec.name
                );
                continue;
            };
            let records = if spec.records == "$" {
                parsed.as_array().cloned().unwrap_or_default()
            } else {
                field(&parsed, &spec.records)
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default()
            };

            let mut s = state.lock().unwrap();
            let key = format!("{}:{}", def.name, spec.name);
            let poll_state = s.connector_state(&key);
            let first_run = poll_state.watermark.is_empty() && poll_state.recent.is_empty();
            let (fresh, new_state) = filter_new(&records, spec, &poll_state);
            s.set_connector_state(&key, new_state);
            if first_run {
                // Live-from-now, like taps: the first poll only establishes
                // the cursor. Backfill is a deliberate act, not a default.
                drop(s);
                eprintln!(
                    "pherd connector '{}' poll '{}': baseline set ({} records seen, live from now)",
                    def.name,
                    spec.name,
                    records.len()
                );
                continue;
            }
            for record in fresh {
                let subject = template(&spec.subject, &|key: &str| {
                    field_str(&record, key).map(|v| sanitize_token(&v))
                });
                let event = PartialEvent {
                    event_type: Some(subject.clone()),
                    subject,
                    payload: Some(record.clone()),
                    source: Some(format!("connector:{}", def.name)),
                    correlation: field_str(&record, &spec.id),
                };
                if let Err(e) = s.ingest(event, 0) {
                    eprintln!("pherd connector '{}': emit failed: {e}", def.name);
                }
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// The escape hatch: supervise any process that emits envelope JSONL on
/// stdout. Token and params ride in env; restart with backoff on exit.
fn run_exec(
    state: &Arc<Mutex<State>>,
    def: &ConnectorDef,
    exec: &ExecSpec,
    cancel: &Arc<AtomicBool>,
) {
    let mut delay = 2u64;
    loop {
        if cancel.load(Ordering::Relaxed) {
            return;
        }
        let mut cmd = std::process::Command::new(&exec.command[0]);
        cmd.args(&exec.command[1..])
            .env("PHER_CONNECTOR", &def.name)
            .env("PHER_CONNECTOR_TOKEN", &def.token)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped());
        for (k, v) in &def.params {
            cmd.env(format!("PHER_PARAM_{}", k.to_uppercase()), v);
        }
        let mut forwarded = false;
        match cmd.spawn() {
            Ok(mut child) => {
                if let Some(stdout) = child.stdout.take() {
                    for line in std::io::BufReader::new(stdout).lines() {
                        if cancel.load(Ordering::Relaxed) {
                            let _ = child.kill();
                            return;
                        }
                        let Ok(line) = line else { break };
                        let Ok(mut event) = serde_json::from_str::<PartialEvent>(line.trim())
                        else {
                            continue;
                        };
                        event.source = Some(format!("connector:{}", def.name));
                        if state.lock().unwrap().ingest(event, 0).is_ok() {
                            forwarded = true;
                        }
                    }
                }
                let _ = child.wait();
            }
            Err(e) => eprintln!("pherd connector '{}': cannot spawn: {e}", def.name),
        }
        eprintln!(
            "pherd connector '{}': exec exited; restarting in {delay}s",
            def.name
        );
        std::thread::sleep(Duration::from_secs(delay));
        delay = if forwarded { 2 } else { (delay * 2).min(60) };
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// CLI-side token resolution. `env:VAR` reads the environment; `cmd:...`
/// runs a command and takes trimmed stdout (this is how secret managers —
/// hem, 1Password, pass — plug in WITHOUT being a dependency); anything
/// else is the literal token.
pub fn resolve_token(spec: &str) -> anyhow::Result<String> {
    if let Some(var) = spec.strip_prefix("env:") {
        return std::env::var(var).map_err(|_| anyhow::anyhow!("env var {var} is not set"));
    }
    if let Some(cmdline) = spec.strip_prefix("cmd:") {
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(cmdline)
            .output()?;
        if !out.status.success() {
            anyhow::bail!(
                "token command failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if token.is_empty() {
            anyhow::bail!("token command produced no output");
        }
        return Ok(token);
    }
    Ok(spec.to_string())
}

pub fn token_fingerprint(token: &str) -> String {
    use sha2::{Digest, Sha256};
    if token.is_empty() {
        return "-".to_string();
    }
    hex::encode(Sha256::digest(token.as_bytes()))[..12].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> PollSpec {
        PollSpec {
            name: "issues".into(),
            url: "https://x/{org}".into(),
            interval: "60s".into(),
            records: "$".into(),
            subject: "sentry.issue.{level}".into(),
            id: "id".into(),
            ts: Some("dateCreated".into()),
            watermark: Some("dateCreated".into()),
        }
    }

    fn rec(id: &str, ts: &str, level: &str) -> Value {
        json!({"id": id, "dateCreated": ts, "level": level})
    }

    #[test]
    fn first_poll_baselines_then_only_new_records_emit() {
        let s = spec();
        let day1 = [rec("1", "2026-08-12T10:00:00Z", "error")];
        let (fresh, st) = filter_new(&day1, &s, &PollState::default());
        assert_eq!(fresh.len(), 1); // engine discards these on first_run
        assert_eq!(st.watermark, "2026-08-12T10:00:00Z");

        // Same record again: watermark + recent both block it.
        let (fresh, st) = filter_new(&day1, &s, &st);
        assert!(fresh.is_empty());

        // A newer and an older record: only the newer passes.
        let day2 = [
            rec("0", "2026-08-12T09:00:00Z", "warning"),
            rec("2", "2026-08-12T11:00:00Z", "error"),
        ];
        let (fresh, st) = filter_new(&day2, &s, &st);
        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0]["id"], json!("2"));
        assert_eq!(st.watermark, "2026-08-12T11:00:00Z");

        // Watermark tie (same ts, unseen id): admitted; repeat of it: blocked.
        let tie = [rec("3", "2026-08-12T11:00:00Z", "error")];
        let (fresh, st) = filter_new(&tie, &s, &st);
        assert_eq!(fresh.len(), 1, "same-second events must not be lost");
        let (fresh, _st) = filter_new(&tie, &s, &st);
        assert!(fresh.is_empty(), "recent-ids window dedups the tie repeat");
    }

    #[test]
    fn numeric_watermarks_compare_numerically() {
        assert!(wm_newer("100", "99"));
        assert!(!wm_newer("99", "100")); // lexicographic would say true
        assert!(wm_newer("2026-08-12T11:00:00Z", "2026-08-12T10:59:59Z"));
    }

    #[test]
    fn templates_and_field_paths() {
        let r = json!({"level": "Error!", "a": {"b": 7}});
        assert_eq!(field_str(&r, "a.b").as_deref(), Some("7"));
        let subj = template("v.{level}.x", &|k| {
            field_str(&r, k).map(|v| sanitize_token(&v))
        });
        assert_eq!(subj, "v.error-.x");
        let auth = template("Bearer {token}", &|k| {
            (k == "token").then(|| "T".to_string())
        });
        assert_eq!(auth, "Bearer T");
    }

    #[test]
    fn builtin_manifests_parse() {
        for (name, text) in BUILTINS {
            let m = Manifest::parse(text).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(&m.name, name);
            assert!(!m.polls.is_empty());
        }
    }

    #[test]
    fn token_resolution_sources() {
        assert_eq!(resolve_token("literal-tok").unwrap(), "literal-tok");
        std::env::set_var("PHER_TEST_TOKEN", "from-env");
        assert_eq!(resolve_token("env:PHER_TEST_TOKEN").unwrap(), "from-env");
        assert!(resolve_token("env:PHER_TEST_MISSING").is_err());
        assert_eq!(resolve_token("cmd:echo from-cmd").unwrap(), "from-cmd");
        assert!(resolve_token("cmd:false").is_err());
    }
}
