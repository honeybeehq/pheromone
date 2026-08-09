use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The canonical event envelope. CloudEvents-compatible field set:
/// `{id, ts, node, source, type, subject, correlation, payload, ttlClass}`.
///
/// `hops` is a Pheromone extension used by the `emit` sink's cycle guard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    pub id: String,
    /// RFC 3339 UTC timestamp. String comparisons order correctly.
    pub ts: String,
    pub node: String,
    pub source: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub subject: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub correlation: Option<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(rename = "ttlClass", skip_serializing_if = "Option::is_none", default)]
    pub ttl_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hops: Option<u32>,
}

impl Envelope {
    /// Minimal constructor for tests and taps; caller fills id/ts/node at ingest.
    pub fn new(subject: impl Into<String>, payload: Value) -> Self {
        let subject = subject.into();
        Envelope {
            id: String::new(),
            ts: String::new(),
            node: String::new(),
            source: String::new(),
            event_type: subject.clone(),
            subject,
            correlation: None,
            payload,
            ttl_class: None,
            hops: None,
        }
    }
}
