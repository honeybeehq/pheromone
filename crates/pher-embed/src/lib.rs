//! Tier 3 (`meaning`): local ONNX embeddings, text projection, redaction.
//!
//! Design contract (LANGUAGE.md / ARCHITECTURE.md):
//! - bge-small class model, 384-dim, runs locally; provider APIs optional,
//!   never required. One embed per event, shared across all subscriptions.
//! - Text projection v1: `type + subject + redacted-stringified payload`,
//!   byte-capped (~2KB). Redaction runs BEFORE projection.
//! - The index is versioned by model id from day one: model swap = reindex.

use std::path::PathBuf;
use std::sync::OnceLock;

use anyhow::Context;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use pher_core::Envelope;
use regex::Regex;

pub const MODEL_ID: &str = "bge-small-en-v1.5-q";
pub const DIM: usize = 384;
const PROJECTION_BYTE_CAP: usize = 2048;

/// The local embedder. Construction downloads the model on first use
/// (~34MB quantized) into the given cache dir; afterwards fully offline.
/// Inference is serialized behind a mutex (fastembed sessions want &mut).
pub struct Embedder {
    model: std::sync::Mutex<TextEmbedding>,
}

impl Embedder {
    pub fn new(cache_dir: PathBuf) -> anyhow::Result<Embedder> {
        let model = TextEmbedding::try_new(
            InitOptions::new(EmbeddingModel::BGESmallENV15Q)
                .with_cache_dir(cache_dir)
                .with_show_download_progress(false),
        )
        .context("cannot load embedding model (first use needs network to fetch ~34MB)")?;
        Ok(Embedder {
            model: std::sync::Mutex::new(model),
        })
    }

    pub fn embed(&self, texts: Vec<String>) -> anyhow::Result<Vec<Vec<f32>>> {
        self.model
            .lock()
            .expect("embedder mutex poisoned")
            .embed(texts, None)
            .context("embedding inference failed")
    }

    pub fn embed_one(&self, text: &str) -> anyhow::Result<Vec<f32>> {
        Ok(self
            .embed(vec![text.to_string()])?
            .into_iter()
            .next()
            .expect("one input, one output"))
    }
}

pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// Text projection v1: what an event "says", fed to the embedder.
pub fn project(event: &Envelope) -> String {
    let payload = redact(&compact_payload(&event.payload));
    let mut out = format!("{} {} {}", event.event_type, event.subject, payload);
    if out.len() > PROJECTION_BYTE_CAP {
        let mut end = PROJECTION_BYTE_CAP;
        while !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
    }
    out
}

/// Flatten a payload into "key: value" prose — closer to what embedding
/// models were trained on than raw JSON punctuation.
fn compact_payload(v: &serde_json::Value) -> String {
    fn walk(prefix: &str, v: &serde_json::Value, out: &mut Vec<String>) {
        match v {
            serde_json::Value::Object(map) => {
                for (k, val) in map {
                    let key = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    walk(&key, val, out);
                }
            }
            serde_json::Value::Array(items) => {
                let rendered: Vec<String> = items
                    .iter()
                    .map(|i| match i {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                out.push(format!("{prefix}: {}", rendered.join(", ")));
            }
            serde_json::Value::String(s) => out.push(format!("{prefix}: {s}")),
            serde_json::Value::Null => {}
            other => out.push(format!("{prefix}: {other}")),
        }
    }
    let mut parts = Vec::new();
    walk("", v, &mut parts);
    parts.join("; ")
}

/// Secret redaction (ported pattern set from the Honeybee haystack pass):
/// Anthropic keys, GitHub tokens, AWS access keys, JWTs, bearer tokens,
/// long generic hex/base64 secrets assigned to key-ish names.
pub fn redact(text: &str) -> String {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    let patterns = PATTERNS.get_or_init(|| {
        [
            r"sk-ant-[A-Za-z0-9_-]{8,}",
            r"\bgh[pousr]_[A-Za-z0-9]{16,}\b",
            r"\bAKIA[0-9A-Z]{16}\b",
            r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}\b",
            r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{16,}",
            r#"(?i)\b(api[_-]?key|token|secret|password|passwd)["']?\s*[:=]\s*["']?[A-Za-z0-9._~+/=-]{12,}"#,
        ]
        .iter()
        .map(|p| Regex::new(p).expect("static pattern"))
        .collect()
    });
    let mut out = text.to_string();
    for re in patterns {
        out = re.replace_all(&out, "[REDACTED]").into_owned();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_secret_shapes() {
        let cases = [
            "key sk-ant-abc123def456ghi789 leaked",
            "gh token ghp_abcdefghijklmnop1234 in log",
            "aws AKIAIOSFODNN7EXAMPLE here",
            "jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9P",
            "Authorization: Bearer abcdefghijklmnopqrstuvwx",
            r#"api_key = "supersecretvalue123456""#,
        ];
        for c in cases {
            let r = redact(c);
            assert!(r.contains("[REDACTED]"), "not redacted: {c} -> {r}");
        }
        assert_eq!(
            redact("plain text, nothing secret"),
            "plain text, nothing secret"
        );
    }

    #[test]
    fn projection_is_capped_and_prose_like() {
        let mut e = Envelope::new(
            "ci.github.run.completed",
            json!({"conclusion": "failure", "branch": "main", "labels": ["ci", "flaky"]}),
        );
        e.event_type = "ci.github.run.completed".into();
        let p = project(&e);
        assert!(p.contains("conclusion: failure"));
        assert!(p.contains("labels: ci, flaky"));
        assert!(p.len() <= PROJECTION_BYTE_CAP);

        let big = json!({"log": "x".repeat(10_000)});
        let e = Envelope::new("crash.sentry.backend", big);
        assert!(project(&e).len() <= PROJECTION_BYTE_CAP);
    }

    #[test]
    fn cosine_basics() {
        assert!((cosine(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
        assert!(cosine(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-6);
        assert_eq!(cosine(&[0.0, 0.0], &[1.0, 0.0]), 0.0);
    }
}
