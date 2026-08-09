//! @pheromone/core — the Pheromone matcher as an in-process Node addon.
//!
//! Same matcher, same test corpus as `pherd`: parse/validate/format
//! subscriptions, dry-run them against events, run `why-not`, and hold a
//! standing `Matcher` — all without a daemon round-trip. This is what powers
//! validate-as-you-type and match preview in TS harnesses.

use napi_derive::napi;
use serde_json::Value;

use pher_core::{Envelope, Subscription};

fn to_err(e: impl std::fmt::Display) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

/// Fill a partial event object (at minimum `{subject}`) into a full envelope,
/// mirroring `pher test` fixture semantics.
fn envelope_from(mut v: Value) -> napi::Result<Envelope> {
    let obj = v
        .as_object_mut()
        .ok_or_else(|| to_err("event must be an object"))?;
    let subject = obj
        .get("subject")
        .and_then(|s| s.as_str())
        .ok_or_else(|| to_err("event is missing 'subject'"))?
        .to_string();
    let defaults = [
        ("id", Value::String(String::new())),
        ("ts", Value::String("2026-01-01T00:00:00Z".into())),
        ("node", Value::String("node".into())),
        ("source", Value::String("test".into())),
        ("type", Value::String(subject)),
    ];
    for (key, default) in defaults {
        obj.entry(key).or_insert(default);
    }
    serde_json::from_value(v).map_err(to_err)
}

/// Parse a subscription string into its canonical JSON form.
#[napi]
pub fn parse(subscription: String) -> napi::Result<Value> {
    Ok(Subscription::parse(&subscription)
        .map_err(to_err)?
        .to_json())
}

/// Convert canonical JSON back to the canonical string form.
#[napi]
pub fn fmt(json: Value) -> napi::Result<String> {
    Ok(Subscription::from_json(&json).map_err(to_err)?.canon())
}

/// Normalize a subscription string to its canonical form.
#[napi]
pub fn canon(subscription: String) -> napi::Result<String> {
    Ok(Subscription::parse(&subscription).map_err(to_err)?.canon())
}

/// Validate a subscription string; returns null when valid, else the parse
/// error message (for validate-as-you-type).
#[napi]
pub fn validate(subscription: String) -> Option<String> {
    Subscription::parse(&subscription)
        .err()
        .map(|e| e.to_string())
}

/// Run one event through one subscription's tier 1-2 cascade; returns the
/// evaluation with outcome and match-explanation block.
#[napi]
pub fn evaluate(subscription: String, event: Value) -> napi::Result<Value> {
    let sub = Subscription::parse(&subscription).map_err(to_err)?;
    let envelope = envelope_from(event)?;
    let eval = pher_core::matcher::evaluate("SUB", &sub, &envelope, None);
    serde_json::to_value(&eval).map_err(to_err)
}

/// One-command diagnosis: why did this event not match this subscription?
#[napi(js_name = "whyNot")]
pub fn why_not(subscription: String, event: Value) -> napi::Result<Value> {
    let sub = Subscription::parse(&subscription).map_err(to_err)?;
    let envelope = envelope_from(event)?;
    Ok(pher_core::matcher::why_not("SUB", &sub, &envelope))
}

/// A standing set of subscriptions with the subject-trie prefilter — the same
/// structure `pherd` runs, embedded in the consumer's process.
#[napi]
pub struct Matcher {
    inner: pher_core::Matcher,
}

#[napi]
impl Matcher {
    #[napi(constructor)]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Matcher {
            inner: pher_core::Matcher::new(),
        }
    }

    /// Register a subscription under an id. Errors on invalid strings.
    #[napi]
    pub fn insert(&mut self, id: String, subscription: String) -> napi::Result<()> {
        let sub = Subscription::parse(&subscription).map_err(to_err)?;
        self.inner.insert(id, sub);
        Ok(())
    }

    #[napi]
    pub fn remove(&mut self, id: String) -> bool {
        self.inner.remove(&id)
    }

    /// Ids of subscriptions whose tier 1-2 cascade matches the event.
    #[napi(js_name = "matchIds")]
    pub fn match_ids(&self, event: Value) -> napi::Result<Vec<String>> {
        let envelope = envelope_from(event)?;
        Ok(self
            .inner
            .match_ids(&envelope)
            .into_iter()
            .map(String::from)
            .collect())
    }

    /// Full evaluations (including rejections with first rejecting tier) for
    /// every trie candidate — match preview UIs want the why, not just the hit.
    #[napi]
    pub fn evaluate(&self, event: Value) -> napi::Result<Value> {
        let envelope = envelope_from(event)?;
        serde_json::to_value(self.inner.evaluate_event(&envelope)).map_err(to_err)
    }

    #[napi]
    pub fn size(&self) -> u32 {
        self.inner.len() as u32
    }
}
