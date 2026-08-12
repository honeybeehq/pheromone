//! Daemon-side tier 3: descriptor scoring and novelty over the event-vector
//! window, backed by pher-embed's local model.
//!
//! Measured quality (crates/pher-embed/examples/quality.rs, bge-small-q):
//! coarse topical descriptors separate cleanly around thresholds 0.6–0.7;
//! fine distinctions ("infra flake, not a code bug") do NOT separate — that
//! is judge-tier (slice 5) territory. Users should set thresholds explicitly;
//! the language default (0.75) is conservative for this model.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use pher_core::{Envelope, Meaning, MeaningKind};
use serde_json::{json, Value};

use crate::store::Paths;

/// Evaporation bound for the in-memory novelty window (principle 6).
const MAX_WINDOW_VECTORS: usize = 20_000;

pub struct Semantic {
    paths: Paths,
    disabled: bool,
    /// The startup prewarm is mid-download; registrations wait it out.
    warming: bool,
    embedder: Option<Arc<pher_embed::Embedder>>,
    /// Descriptor embeddings per subscription id.
    desc_vecs: HashMap<String, Vec<Vec<f32>>>,
    /// Event-embedding window for novelty: (unix ts, event id, vector),
    /// oldest first.
    window: VecDeque<(u64, String, Vec<f32>)>,
}

impl Semantic {
    pub fn new(paths: Paths) -> Semantic {
        let disabled = std::env::var("PHER_EMBED").is_ok_and(|v| v == "off" || v == "0");
        let mut window = VecDeque::new();
        if let Ok(text) = std::fs::read_to_string(paths.vectors()) {
            for line in text.lines() {
                let Ok(v) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                let (Some(ts), Some(vec)) = (v.get("ts").and_then(|t| t.as_u64()), v.get("vec"))
                else {
                    continue;
                };
                let Ok(vec) = serde_json::from_value::<Vec<f32>>(vec.clone()) else {
                    continue;
                };
                let id = v
                    .get("id")
                    .and_then(|i| i.as_str())
                    .unwrap_or_default()
                    .to_string();
                window.push_back((ts, id, vec));
                if window.len() > MAX_WINDOW_VECTORS {
                    window.pop_front();
                }
            }
        }
        Semantic {
            paths,
            disabled,
            warming: false,
            embedder: None,
            desc_vecs: HashMap::new(),
            window,
        }
    }

    fn ensure_embedder(&mut self) -> Result<Arc<pher_embed::Embedder>, String> {
        if self.disabled {
            return Err("semantic tier disabled (PHER_EMBED=off)".to_string());
        }
        if let Some(e) = &self.embedder {
            return Ok(Arc::clone(e));
        }
        if self.warming {
            // Racing the prewarm's model download would trip fastembed's
            // file lock and surface as gibberish; say what's happening.
            return Err(
                "meaning tier is warming up (model downloading) — retry in a few seconds"
                    .to_string(),
            );
        }
        let embedder =
            pher_embed::Embedder::new(self.paths.models()).map_err(|e| format!("{e:#}"))?;
        let arc = Arc::new(embedder);
        self.embedder = Some(Arc::clone(&arc));
        Ok(arc)
    }

    pub fn set_warming(&mut self, warming: bool) {
        self.warming = warming;
    }

    pub fn enabled(&self) -> bool {
        !self.disabled
    }

    /// Build an embedder with NO daemon lock held — model download and ONNX
    /// load can take many seconds on a cold home, and doing that lazily
    /// inside the state lock froze the entire daemon at first `meaning`
    /// registration. The startup prewarm calls this from its own thread.
    pub fn build_embedder(paths: &Paths) -> Result<Arc<pher_embed::Embedder>, String> {
        pher_embed::Embedder::new(paths.models())
            .map(Arc::new)
            .map_err(|e| format!("{e:#}"))
    }

    /// Install a prewarmed embedder (no-op if disabled or already warm).
    pub fn install(&mut self, embedder: Arc<pher_embed::Embedder>) {
        if !self.disabled && self.embedder.is_none() {
            self.embedder = Some(embedder);
        }
    }

    /// Embed a meaning subscription's descriptors at registration time.
    pub fn on_register(&mut self, sub_id: &str, meaning: &Meaning) -> Result<(), String> {
        match &meaning.kind {
            MeaningKind::Descriptors(descriptors) => {
                let embedder = self.ensure_embedder()?;
                let vecs = embedder
                    .embed(descriptors.clone())
                    .map_err(|e| format!("{e:#}"))?;
                self.desc_vecs.insert(sub_id.to_string(), vecs);
                Ok(())
            }
            MeaningKind::Novel { .. } => {
                // Nothing to embed up front, but the model must be loadable.
                self.ensure_embedder().map(|_| ())
            }
        }
    }

    pub fn on_remove(&mut self, sub_id: &str) {
        self.desc_vecs.remove(sub_id);
    }

    pub fn embed_event(&mut self, event: &Envelope) -> Result<Vec<f32>, String> {
        let embedder = self.ensure_embedder()?;
        embedder
            .embed_one(&pher_embed::project(event))
            .map_err(|e| format!("{e:#}"))
    }

    /// Score one meaning clause against an event vector. Returns
    /// (passed, meaning-record for the match block).
    pub fn score(
        &self,
        sub_id: &str,
        meaning: &Meaning,
        event_id: &str,
        event_vec: &[f32],
        now_unix: u64,
    ) -> (bool, Value) {
        match &meaning.kind {
            MeaningKind::Descriptors(descriptors) => {
                let Some(vecs) = self.desc_vecs.get(sub_id) else {
                    return (
                        false,
                        json!({ "error": "descriptor vectors unavailable (embedder was down at registration?)" }),
                    );
                };
                let mut best = f32::MIN;
                let mut best_i = 0;
                for (i, dv) in vecs.iter().enumerate() {
                    let s = pher_embed::cosine(dv, event_vec);
                    if s > best {
                        best = s;
                        best_i = i;
                    }
                }
                let passed = f64::from(best) > meaning.threshold;
                (
                    passed,
                    json!({
                        "descriptor": descriptors[best_i],
                        "score": best,
                        "threshold": meaning.threshold,
                        "model": pher_embed::MODEL_ID,
                    }),
                )
            }
            MeaningKind::Novel { over } => {
                let cutoff = now_unix.saturating_sub(over.secs());
                let mut nearest = 0.0f32;
                let mut considered = 0usize;
                for (ts, id, vec) in self.window.iter().rev() {
                    if *ts < cutoff {
                        break; // window is ordered oldest-first
                    }
                    if id == event_id {
                        continue; // an event is never its own precedent
                    }
                    considered += 1;
                    let s = pher_embed::cosine(vec, event_vec);
                    if s > nearest {
                        nearest = s;
                    }
                }
                let score = 1.0 - nearest;
                let passed = f64::from(score) > meaning.threshold;
                (
                    passed,
                    json!({
                        "novel": true,
                        "score": score,
                        "nearestSimilarity": nearest,
                        "threshold": meaning.threshold,
                        "over": over.text(),
                        "windowSize": considered,
                        "model": pher_embed::MODEL_ID,
                    }),
                )
            }
        }
    }

    /// Add an event's vector to the novelty window (memory + disk).
    pub fn record_event(&mut self, now_unix: u64, event_id: &str, vec: Vec<f32>) {
        let line =
            json!({ "ts": now_unix, "id": event_id, "model": pher_embed::MODEL_ID, "vec": vec });
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.paths.vectors())
        {
            use std::io::Write;
            let _ = writeln!(f, "{line}");
        }
        self.window.push_back((now_unix, event_id.to_string(), vec));
        while self.window.len() > MAX_WINDOW_VECTORS {
            self.window.pop_front();
        }
    }

    /// Evaporate vectors older than the cutoff (memory + disk rewrite).
    pub fn gc(&mut self, cutoff_unix: u64) {
        while self
            .window
            .front()
            .is_some_and(|(ts, _, _)| *ts < cutoff_unix)
        {
            self.window.pop_front();
        }
        let Ok(text) = std::fs::read_to_string(self.paths.vectors()) else {
            return;
        };
        let kept: Vec<&str> = text
            .lines()
            .filter(|l| {
                serde_json::from_str::<Value>(l)
                    .ok()
                    .and_then(|v| v.get("ts").and_then(|t| t.as_u64()))
                    .is_some_and(|ts| ts >= cutoff_unix)
            })
            .collect();
        if kept.len() != text.lines().count() {
            let mut body = kept.join("\n");
            if !body.is_empty() {
                body.push('\n');
            }
            let tmp = self.paths.vectors().with_extension("tmp");
            if std::fs::write(&tmp, body).is_ok() {
                let _ = std::fs::rename(&tmp, self.paths.vectors());
            }
        }
    }

    pub fn status(&self) -> Value {
        if self.disabled {
            return json!({ "enabled": false, "reason": "PHER_EMBED=off" });
        }
        json!({
            "enabled": true,
            "model": pher_embed::MODEL_ID,
            "loaded": self.embedder.is_some(),
            "descriptorSets": self.desc_vecs.len(),
            "windowVectors": self.window.len(),
        })
    }
}
