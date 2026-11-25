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

use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    #[allow(dead_code)]
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
    /// Record field holding the vendor's event timestamp, carried as the
    /// envelope's `sourceTs` (ingest `ts` stays authoritative). Dotted.
    #[serde(default)]
    pub ts: Option<String>,
    /// Record field to watermark on (only records with field > stored
    /// watermark are emitted). Compared exactly as numbers when both sides are
    /// numbers, chronologically when both are RFC 3339 timestamps, else as strings.
    #[serde(default)]
    pub watermark: Option<String>,
    #[serde(default)]
    pub paginate: Paginate,
    #[serde(default = "default_page_param")]
    pub page_param: String,
    #[serde(default)]
    pub has_more: Option<String>,
    #[serde(default = "default_max_pages")]
    pub max_pages: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Paginate {
    #[default]
    Link,
    AfterId,
    None,
}

fn default_records() -> String {
    "$".to_string()
}

fn default_page_param() -> String {
    "starting_after".to_string()
}

fn default_max_pages() -> u32 {
    10
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
    /// Recently seen (vendor id, watermark value) pairs; bare vendor ids
    /// for polls without a watermark. Covers watermark ties and wm-less polls.
    #[serde(default)]
    pub recent: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub draining_watermark: String,
    #[serde(default)]
    pub baselined: bool,
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

fn wm_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    match (WmKey::parse(a), WmKey::parse(b)) {
        (WmKey::Number(x), WmKey::Number(y)) => x.cmp(&y),
        (WmKey::Instant(x), WmKey::Instant(y)) => x.cmp(&y),
        _ => a.cmp(b),
    }
}

fn wm_newer(candidate: &str, stored: &str) -> bool {
    stored.is_empty() || wm_cmp(candidate, stored) == std::cmp::Ordering::Greater
}

fn sort_oldest_first<T>(items: &mut [(String, String, T)]) {
    let kinds: HashSet<_> = items
        .iter()
        .filter(|item| !item.0.is_empty())
        .map(|item| std::mem::discriminant(&WmKey::parse(&item.0)))
        .collect();
    if kinds.len() <= 1 {
        items.sort_by(|a, b| wm_cmp(&a.0, &b.0));
    } else {
        items.sort_by(|a, b| a.0.cmp(&b.0));
    }
}

enum WmKey {
    Number(Decimal),
    Instant(std::time::SystemTime),
    Text,
}

impl WmKey {
    fn parse(text: &str) -> WmKey {
        if let Some(number) = Decimal::parse(text) {
            WmKey::Number(number)
        } else if let Some(instant) = rfc3339_instant(text) {
            WmKey::Instant(instant)
        } else {
            WmKey::Text
        }
    }
}

#[derive(PartialEq, Eq)]
struct Decimal {
    negative: bool,
    digits: String,
    magnitude: i64,
}

impl Decimal {
    fn parse(text: &str) -> Option<Decimal> {
        let (negative, unsigned) = match text.as_bytes().first()? {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(at) => (&unsigned[..at], unsigned[at + 1..].parse::<i32>().ok()?),
            None => (unsigned, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if (whole.is_empty() && fraction.is_empty())
            || !whole.bytes().all(|b| b.is_ascii_digit())
            || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let all = format!("{whole}{fraction}");
        let significant = all.trim_start_matches('0');
        let leading_zeros = all.len() - significant.len();
        let digits = significant.trim_end_matches('0').to_string();
        if digits.is_empty() {
            return Some(Decimal {
                negative: false,
                digits,
                magnitude: 0,
            });
        }
        let magnitude = whole.len() as i64 - leading_zeros as i64 + i64::from(exponent);
        Some(Decimal {
            negative,
            digits,
            magnitude,
        })
    }

    fn sign(&self) -> i8 {
        match (self.digits.is_empty(), self.negative) {
            (true, _) => 0,
            (false, true) => -1,
            (false, false) => 1,
        }
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let by_sign = self.sign().cmp(&other.sign());
        if by_sign != std::cmp::Ordering::Equal || self.sign() == 0 {
            return by_sign;
        }
        let by_size = self
            .magnitude
            .cmp(&other.magnitude)
            .then_with(|| self.digits.cmp(&other.digits));
        if self.negative {
            by_size.reverse()
        } else {
            by_size
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn record_version(record: &Value, spec: &PollSpec) -> String {
    spec.watermark
        .as_deref()
        .and_then(|f| field_str(record, f))
        .unwrap_or_default()
}

fn seen_key(vendor_id: &str, version: &str) -> String {
    if version.is_empty() {
        vendor_id.to_string()
    } else {
        format!("{vendor_id}\u{1f}{version}")
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
    let mut fresh: Vec<(String, String, Value)> = Vec::new();
    let mut batch: HashSet<String> = HashSet::new();
    for r in records {
        let Some(id) = field_str(r, &spec.id) else {
            continue; // no id, no dedup, no admission
        };
        let version = record_version(r, spec);
        let key = seen_key(&id, &version);
        let legacy_tie = !state.watermark.is_empty()
            && wm_cmp(&version, &state.watermark).is_eq()
            && state.recent.contains(&id);
        if legacy_tie || state.recent.contains(&key) || batch.contains(&key) {
            continue;
        }
        if spec.watermark.is_some() {
            // Admit strictly-newer, AND watermark ties (two events in the
            // same second) — the recent-ids window dedups tie repeats.
            let admissible =
                state.watermark.is_empty() || wm_cmp(&version, &state.watermark).is_ge();
            if !admissible {
                continue;
            }
        }
        batch.insert(key.clone());
        fresh.push((version, key, r.clone()));
    }
    sort_oldest_first(&mut fresh);
    for (wm, key, _) in &fresh {
        if wm_newer(wm, &new_state.watermark) {
            new_state.watermark = wm.clone();
        }
        new_state.recent.push(key.clone());
    }
    let excess = new_state.recent.len().saturating_sub(RECENT_IDS);
    if excess > 0 {
        new_state.recent.drain(..excess);
    }
    (fresh.into_iter().map(|(_, _, r)| r).collect(), new_state)
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
    let mut due: HashMap<String, u64> = HashMap::new();
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
            if let Err(e) = poll_once(state, def, manifest, spec, cancel) {
                eprintln!("pherd connector '{}' poll '{}': {e}", def.name, spec.name);
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

struct Page {
    records: Vec<Value>,
    next: Next,
}

enum Next {
    End,
    More(String),
    Unreachable(String),
}

struct Fetched {
    records: Vec<Value>,
    resume: Option<String>,
    complete: bool,
}

struct FetchError {
    status: Option<u16>,
    message: String,
}

fn poll_once(
    state: &Arc<Mutex<State>>,
    def: &ConnectorDef,
    manifest: &Manifest,
    spec: &PollSpec,
    cancel: &AtomicBool,
) -> Result<(), String> {
    let key = format!("{}:{}", def.name, spec.name);
    let stored = state.lock().unwrap().connector_state(&key);
    let first_run = !stored.baselined
        && stored.watermark.is_empty()
        && stored.recent.is_empty()
        && stored.resume.is_none();
    let fetched = fetch_pages(def, manifest, spec, &stored, first_run)?;
    let (fresh, filtered) = filter_new(&fetched.records, spec, &stored);

    if first_run {
        let mut baseline = filtered;
        baseline.resume = None;
        baseline.baselined = true;
        state.lock().unwrap().set_connector_state(&key, baseline);
        eprintln!(
            "pherd connector '{}' poll '{}': baseline set ({} records seen, live from now)",
            def.name,
            spec.name,
            fetched.records.len()
        );
        return Ok(());
    }

    let mut s = state.lock().unwrap();
    if cancel.load(Ordering::Relaxed) {
        return Ok(());
    }
    for record in &fresh {
        let Some(vendor_id) = field_str(record, &spec.id) else {
            continue;
        };
        let subject = template(&spec.subject, &|key: &str| {
            field_str(record, key).map(|v| sanitize_token(&v))
        });
        if !pher_core::SubjectPattern::parse(&subject).is_ok_and(|p| p.is_concrete()) {
            eprintln!(
                "pherd connector '{}' poll '{}': skipping record {vendor_id}: invalid subject '{subject}'",
                def.name, spec.name
            );
            continue;
        }
        let version = record_version(record, spec);
        let source_ts = vendor_ts(record, spec);
        if let VendorTs::Unreadable(raw) = &source_ts {
            eprintln!(
                "pherd connector '{}' poll '{}': record {vendor_id} admitted without sourceTs: unreadable '{}' value {raw}",
                def.name,
                spec.name,
                spec.ts.as_deref().unwrap_or_default()
            );
        }
        let event = PartialEvent {
            event_type: Some(subject.clone()),
            subject,
            payload: Some(record.clone()),
            source: Some(format!("connector:{}", def.name)),
            correlation: Some(vendor_id.clone()),
            source_ts: source_ts.into_option().map(Value::String),
        };
        s.ingest_once(once_key(&def.name, &spec.name, &vendor_id, &version), event)
            .map_err(|e| format!("emit failed, cursor held for retry: {e}"))?;
    }
    s.set_connector_state(&key, advance(&stored, filtered, &fetched));
    Ok(())
}

fn advance(stored: &PollState, filtered: PollState, fetched: &Fetched) -> PollState {
    let mut highest = stored.draining_watermark.clone();
    if wm_newer(&filtered.watermark, &highest) {
        highest = filtered.watermark;
    }
    if fetched.complete && fetched.resume.is_none() {
        PollState {
            watermark: highest,
            recent: filtered.recent,
            resume: None,
            draining_watermark: String::new(),
            baselined: true,
        }
    } else {
        PollState {
            watermark: stored.watermark.clone(),
            recent: filtered.recent,
            resume: fetched.resume.clone(),
            draining_watermark: highest,
            baselined: true,
        }
    }
}

enum VendorTs {
    Unset,
    Read(String),
    Unreadable(String),
}

impl VendorTs {
    fn into_option(self) -> Option<String> {
        match self {
            VendorTs::Read(ts) => Some(ts),
            VendorTs::Unset | VendorTs::Unreadable(_) => None,
        }
    }
}

fn vendor_ts(record: &Value, spec: &PollSpec) -> VendorTs {
    let Some(path) = spec.ts.as_deref() else {
        return VendorTs::Unset;
    };
    match field(record, path) {
        None => VendorTs::Unreadable("(missing)".to_string()),
        Some(value) => match normalize_source_ts(value) {
            Some(ts) => VendorTs::Read(ts),
            None => VendorTs::Unreadable(truncate_for_log(&value.to_string())),
        },
    }
}

pub(crate) fn truncate_for_log(text: &str) -> String {
    const MAX: usize = 64;
    match text.char_indices().nth(MAX) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text.to_string(),
    }
}

pub(crate) fn normalize_source_ts(value: &Value) -> Option<String> {
    let instant = match value {
        Value::Number(n) => epoch_instant(&n.to_string())?,
        Value::String(s) => {
            let s = s.trim();
            epoch_instant(s).or_else(|| rfc3339_instant(s))?
        }
        _ => return None,
    };
    let year_10000 = std::time::UNIX_EPOCH + Duration::from_secs(253_402_300_800);
    if instant >= year_10000 {
        return None;
    }
    Some(humantime::format_rfc3339_millis(instant).to_string())
}

fn epoch_instant(text: &str) -> Option<std::time::SystemTime> {
    const MS_THRESHOLD: u64 = 100_000_000_000;
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let whole: u64 = whole.parse().ok()?;
    let fraction_nanos = |digits: &str, scale: u32| -> u64 {
        let digits = &digits[..digits.len().min(scale as usize)];
        digits.parse::<u64>().unwrap_or(0) * 10u64.pow(scale - digits.len() as u32)
    };
    let duration = if whole >= MS_THRESHOLD {
        Duration::from_millis(whole) + Duration::from_nanos(fraction_nanos(fraction, 6))
    } else {
        Duration::from_secs(whole) + Duration::from_nanos(fraction_nanos(fraction, 9))
    };
    std::time::UNIX_EPOCH.checked_add(duration)
}

fn rfc3339_instant(s: &str) -> Option<std::time::SystemTime> {
    if !s.is_ascii() || s.len() < 20 || s.as_bytes()[10] != b'T' {
        return None;
    }
    if let Some(utc) = s.strip_suffix(['Z', 'z']) {
        return humantime::parse_rfc3339(&format!("{utc}Z")).ok();
    }
    let (local, offset) = s.split_at(s.len() - 6);
    let o = offset.as_bytes();
    if !matches!(o[0], b'+' | b'-') || o[3] != b':' {
        return None;
    }
    let hours: u64 = offset[1..3].parse().ok()?;
    let minutes: u64 = offset[4..6].parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    let shift = Duration::from_secs(hours * 3600 + minutes * 60);
    let as_if_utc = humantime::parse_rfc3339(&format!("{local}Z")).ok()?;
    let instant = if o[0] == b'+' {
        as_if_utc.checked_sub(shift)?
    } else {
        as_if_utc.checked_add(shift)?
    };
    (instant >= std::time::UNIX_EPOCH).then_some(instant)
}

fn once_key(connector: &str, poll: &str, vendor_id: &str, version: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("{connector}\0{poll}\0{vendor_id}\0{version}").as_bytes());
    format!("connector|{}", &hex::encode(digest)[..32])
}

struct Secret<'a>(&'a str);

impl Secret<'_> {
    fn forms(&self) -> Vec<(String, &'static str)> {
        if self.0.is_empty() {
            return Vec::new();
        }
        let encoded = percent_encode(self.0);
        let lower = encoded.to_ascii_lowercase();
        let mut forms = vec![(self.0.to_string(), "{token}")];
        if encoded != self.0 {
            if lower != encoded && lower != self.0 {
                forms.push((lower, "{token_pct_lower}"));
            }
            forms.push((encoded, "{token_pct}"));
        }
        forms
    }

    fn seal(&self, url: &str) -> String {
        self.forms()
            .iter()
            .fold(url.to_string(), |acc, (value, slot)| {
                acc.replace(value, slot)
            })
    }

    fn unseal(&self, url: &str) -> String {
        self.forms()
            .iter()
            .rev()
            .fold(url.to_string(), |acc, (value, slot)| {
                acc.replace(slot, value)
            })
    }

    fn redact(&self, text: &str) -> String {
        self.forms()
            .iter()
            .fold(text.to_string(), |acc, (value, _)| {
                acc.replace(value, "***")
            })
    }
}

fn fetch_pages(
    def: &ConnectorDef,
    manifest: &Manifest,
    spec: &PollSpec,
    stored: &PollState,
    first_run: bool,
) -> Result<Fetched, String> {
    let vars = |key: &str| -> Option<String> {
        if key == "token" {
            Some(def.token.clone())
        } else {
            def.params.get(key).cloned()
        }
    };
    let base = template(&spec.url, &vars);
    let auth = manifest
        .auth
        .as_ref()
        .map(|a| (a.header.clone(), template(&a.format, &vars)));
    let secret = Secret(&def.token);
    let max_pages = if first_run { 1 } else { spec.max_pages.max(1) };
    let mut cursor = stored.resume.clone();
    let mut records = Vec::new();
    for fetched_pages in 0..max_pages {
        let url = match &cursor {
            Some(c) => page_url(spec, &base, &secret.unseal(c)),
            None => base.clone(),
        };
        let fetched_page = fetch_page(&url, &base, auth.as_ref(), spec).map_err(|e| FetchError {
            status: e.status,
            message: secret.redact(&e.message),
        });
        let page = match fetched_page {
            Ok(page) => page,
            Err(e)
                if fetched_pages == 0
                    && cursor.is_some()
                    && e.status
                        .is_some_and(|code| matches!(code, 400 | 404 | 410 | 422)) =>
            {
                eprintln!(
                    "pherd connector '{}' poll '{}': resume cursor rejected ({}), ending the drain; older records past it are unreachable",
                    def.name, spec.name, e.message
                );
                return Ok(Fetched {
                    records,
                    resume: None,
                    complete: true,
                });
            }
            Err(e) if fetched_pages == 0 => return Err(e.message),
            Err(e) => {
                eprintln!(
                    "pherd connector '{}' poll '{}': page fetch failed, resuming next poll: {}",
                    def.name, spec.name, e.message
                );
                return Ok(Fetched {
                    records,
                    resume: cursor,
                    complete: false,
                });
            }
        };
        let reached_known = page.records.iter().any(|r| is_known(r, spec, stored));
        let empty = page.records.is_empty();
        records.extend(page.records);
        if first_run || reached_known || empty {
            return Ok(Fetched {
                records,
                resume: None,
                complete: true,
            });
        }
        match page.next {
            Next::End => {
                return Ok(Fetched {
                    records,
                    resume: None,
                    complete: true,
                })
            }
            Next::Unreachable(reason) => {
                eprintln!(
                    "pherd connector '{}' poll '{}': more pages exist but {}; watermark held",
                    def.name,
                    spec.name,
                    secret.redact(&reason)
                );
                return Ok(Fetched {
                    records,
                    resume: None,
                    complete: false,
                });
            }
            Next::More(next) => cursor = Some(secret.seal(&next)),
        }
    }
    Ok(Fetched {
        records,
        resume: cursor,
        complete: false,
    })
}

fn is_known(record: &Value, spec: &PollSpec, stored: &PollState) -> bool {
    match &spec.watermark {
        Some(wm_field) => {
            let wm = field_str(record, wm_field).unwrap_or_default();
            !stored.watermark.is_empty() && wm_cmp(&wm, &stored.watermark).is_lt()
        }
        None => {
            field_str(record, &spec.id).is_some_and(|id| stored.recent.contains(&seen_key(&id, "")))
        }
    }
}

fn page_url(spec: &PollSpec, base: &str, cursor: &str) -> String {
    match spec.paginate {
        Paginate::AfterId => {
            let sep = if base.contains('?') { '&' } else { '?' };
            format!("{base}{sep}{}={}", spec.page_param, percent_encode(cursor))
        }
        Paginate::Link | Paginate::None => cursor.to_string(),
    }
}

fn fetch_page(
    url: &str,
    base: &str,
    auth: Option<&(String, String)>,
    spec: &PollSpec,
) -> Result<Page, FetchError> {
    let mut req = ureq::get(url)
        .timeout(Duration::from_secs(20))
        .set("accept", "application/json")
        .set("user-agent", "pher-connector");
    if let Some((header, value)) = auth {
        req = req.set(header, value);
    }
    let failed = |message: String| FetchError {
        status: None,
        message,
    };
    let resp = req.call().map_err(|e| match e {
        ureq::Error::Status(code, _) => FetchError {
            status: Some(code),
            message: format!("HTTP {code}"),
        },
        other => failed(other.to_string()),
    })?;
    let link = resp.header("link").map(str::to_string);
    let body = resp.into_string().map_err(|e| failed(e.to_string()))?;
    let parsed: Value =
        serde_json::from_str(&body).map_err(|_| failed("response is not JSON".to_string()))?;
    let records = if spec.records == "$" {
        parsed.as_array().cloned().unwrap_or_default()
    } else {
        field(&parsed, &spec.records)
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
    };
    let next = match spec.paginate {
        Paginate::None => Next::End,
        Paginate::Link => match link.as_deref().and_then(next_link) {
            None => Next::End,
            Some(target) => match resolve_link(&target, base) {
                Some(url) if same_origin(&url, base) => Next::More(url),
                _ => Next::Unreachable(format!(
                    "the next-page link is not on {}",
                    origin(base).unwrap_or("the poll's origin")
                )),
            },
        },
        Paginate::AfterId => {
            let more = match &spec.has_more {
                Some(path) => field(&parsed, path).and_then(Value::as_bool) == Some(true),
                None => !records.is_empty(),
            };
            match (more, records.last().and_then(|r| field_str(r, &spec.id))) {
                (false, _) => Next::End,
                (true, Some(id)) => Next::More(id),
                (true, None) => Next::Unreachable("the last record has no id".to_string()),
            }
        }
    };
    Ok(Page { records, next })
}

fn next_link(header: &str) -> Option<String> {
    let mut rest = header;
    while let Some(open) = rest.find('<') {
        let after = &rest[open + 1..];
        let close = after.find('>')?;
        let target = &after[..close];
        let tail = &after[close + 1..];
        let params_end = tail.find('<').unwrap_or(tail.len());
        let params: Vec<(String, String)> = tail[..params_end]
            .split([';', ','])
            .filter_map(|p| {
                let (k, v) = p.trim().split_once('=')?;
                Some((
                    k.trim().to_ascii_lowercase(),
                    v.trim().trim_matches('"').to_string(),
                ))
            })
            .collect();
        let is_next = params
            .iter()
            .any(|(k, v)| k == "rel" && v.split_whitespace().any(|r| r == "next"));
        let empty = params.iter().any(|(k, v)| k == "results" && v == "false");
        if is_next && !empty {
            return Some(target.to_string());
        }
        rest = &tail[params_end..];
    }
    None
}

fn resolve_link(target: &str, base: &str) -> Option<String> {
    if target.contains("://") {
        Some(target.to_string())
    } else if target.starts_with('/') && !target.starts_with("//") {
        Some(format!("{}{target}", origin(base)?))
    } else {
        None
    }
}

fn origin(url: &str) -> Option<&str> {
    let scheme_end = url.find("://")? + 3;
    let host_end = url[scheme_end..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |i| scheme_end + i);
    Some(&url[..host_end])
}

fn same_origin(a: &str, b: &str) -> bool {
    match (origin(a), origin(b)) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        _ => false,
    }
}

fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
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
    use serde_json::json;

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
            paginate: Paginate::Link,
            page_param: default_page_param(),
            has_more: None,
            max_pages: default_max_pages(),
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
    fn reoccurrence_with_newer_watermark_is_admitted() {
        let s = spec();
        let seen = [rec("7", "2026-08-12T10:00:00Z", "error")];
        let (_, st) = filter_new(&seen, &s, &PollState::default());

        let regressed = [rec("7", "2026-08-12T12:00:00Z", "error")];
        let (fresh, st) = filter_new(&regressed, &s, &st);
        assert_eq!(fresh.len(), 1, "a regressed issue must not be dropped");
        assert_eq!(st.watermark, "2026-08-12T12:00:00Z");

        let (fresh, _) = filter_new(&regressed, &s, &st);
        assert!(fresh.is_empty(), "the same version is still a duplicate");
    }

    #[test]
    fn numeric_watermarks_compare_numerically() {
        assert!(wm_newer("100", "99"));
        assert!(!wm_newer("99", "100")); // lexicographic would say true
        assert!(wm_newer("2026-08-12T11:00:00Z", "2026-08-12T10:59:59Z"));
        assert!(wm_newer("9007199254740993", "9007199254740992"));
        assert!(wm_newer("1e3", "999.5"));
        assert!(wm_newer("-1", "-2"));
        assert!(wm_newer(".5", "0.4"));
        assert!(wm_cmp("10.0", "1e1").is_eq());
        assert!(wm_newer(
            "2026-08-12T09:45:00-01:00",
            "2026-08-12T10:00:00Z"
        ));
        assert!(wm_newer("2026-08-12T11:00:00.500Z", "2026-08-12T11:00:00Z"));
        assert!(wm_cmp("2026-08-12T12:30:00+02:00", "2026-08-12T10:30:00Z").is_eq());

        let mut s = spec();
        s.watermark = Some("seq".into());
        let batch: Vec<Value> = [11, 9, 10]
            .iter()
            .map(|n| json!({"id": format!("n{n}"), "seq": n}))
            .collect();
        let (fresh, st) = filter_new(&batch, &s, &PollState::default());
        let order: Vec<_> = fresh.iter().map(|r| r["seq"].clone()).collect();
        assert_eq!(order, [json!(9), json!(10), json!(11)]);
        assert_eq!(st.watermark, "11");
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
