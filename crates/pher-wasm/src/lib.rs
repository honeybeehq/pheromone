//! @pheromone/wasm — the tiers 1-2 matcher compiled to WebAssembly.
//!
//! This is the "bring the subscription to the data" wedge: the same matcher
//! (same corpus) running in edge middleware, log-shipper plugins, or a
//! browser playground, so only survivors cross the wire. All functions take
//! and return JSON strings — the lowest-friction ABI across wasm hosts.

use wasm_bindgen::prelude::*;

use pher_core::{Envelope, Subscription};

fn err_js(e: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&e.to_string())
}

fn envelope_from(event_json: &str) -> Result<Envelope, JsValue> {
    let mut v: serde_json::Value =
        serde_json::from_str(event_json).map_err(|e| err_js(format!("bad event JSON: {e}")))?;
    let obj = v
        .as_object_mut()
        .ok_or_else(|| err_js("event must be an object"))?;
    let subject = obj
        .get("subject")
        .and_then(|s| s.as_str())
        .ok_or_else(|| err_js("event is missing 'subject'"))?
        .to_string();
    let defaults = [
        ("id", serde_json::Value::String(String::new())),
        (
            "ts",
            serde_json::Value::String("2026-01-01T00:00:00Z".into()),
        ),
        ("node", serde_json::Value::String("wasm".into())),
        ("source", serde_json::Value::String("wasm".into())),
        ("type", serde_json::Value::String(subject)),
    ];
    for (key, default) in defaults {
        obj.entry(key).or_insert(default);
    }
    serde_json::from_value(v).map_err(err_js)
}

/// Parse a subscription string; returns the canonical JSON form.
#[wasm_bindgen]
pub fn parse(subscription: &str) -> Result<String, JsValue> {
    let sub = Subscription::parse(subscription).map_err(err_js)?;
    Ok(sub.to_json().to_string())
}

/// Normalize to the canonical string form.
#[wasm_bindgen]
pub fn canon(subscription: &str) -> Result<String, JsValue> {
    Ok(Subscription::parse(subscription).map_err(err_js)?.canon())
}

/// Returns null when valid, else the parse error (validate-as-you-type).
#[wasm_bindgen]
pub fn validate(subscription: &str) -> Option<String> {
    Subscription::parse(subscription)
        .err()
        .map(|e| e.to_string())
}

/// Tier 1-2 evaluation of one event (JSON) against one subscription string.
/// Returns the evaluation JSON with outcome + match-explanation block.
#[wasm_bindgen]
pub fn evaluate(subscription: &str, event_json: &str) -> Result<String, JsValue> {
    let sub = Subscription::parse(subscription).map_err(err_js)?;
    let envelope = envelope_from(event_json)?;
    let eval = pher_core::matcher::evaluate("SUB", &sub, &envelope, None);
    serde_json::to_string(&eval).map_err(err_js)
}

/// Filter-at-source: does this event survive tiers 1-2? (`meaning`/`judge`
/// clauses report `pending`, which counts as survival — the expensive tiers
/// run upstream where budget lives.)
#[wasm_bindgen]
pub fn survives(subscription: &str, event_json: &str) -> Result<bool, JsValue> {
    let sub = Subscription::parse(subscription).map_err(err_js)?;
    let envelope = envelope_from(event_json)?;
    use pher_core::matcher::{check, Check};
    Ok(matches!(
        check(&sub, &envelope, None),
        Check::Matched | Check::Pending
    ))
}

/// Why-not diagnosis, same record as the daemon's.
#[wasm_bindgen(js_name = whyNot)]
pub fn why_not(subscription: &str, event_json: &str) -> Result<String, JsValue> {
    let sub = Subscription::parse(subscription).map_err(err_js)?;
    let envelope = envelope_from(event_json)?;
    Ok(pher_core::matcher::why_not("SUB", &sub, &envelope).to_string())
}
