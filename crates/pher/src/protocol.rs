use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One JSON line per request over the unix socket. Most ops answer with a
/// single JSON line; `tail` streams lines until the client disconnects.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
pub enum Request {
    Emit {
        event: PartialEvent,
    },
    When {
        string: String,
        #[serde(default)]
        options: Vec<String>,
    },
    Ls,
    Rm {
        id: String,
    },
    Tail {
        #[serde(default)]
        after: Option<u64>,
        #[serde(default)]
        subject: Option<String>,
    },
    Why {
        #[serde(rename = "deliveryId")]
        delivery_id: String,
    },
    WhyNot {
        sub: String,
        event: String,
    },
    Status,
}

/// Envelope fields a producer may supply; the daemon fills id/ts/node/seq.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialEvent {
    pub subject: String,
    #[serde(default)]
    pub payload: Option<Value>,
    #[serde(rename = "type", default)]
    pub event_type: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub correlation: Option<String>,
}

pub fn ok(mut extra: Value) -> Value {
    let obj = extra.as_object_mut().expect("ok() takes an object");
    obj.insert("ok".to_string(), Value::Bool(true));
    extra
}

pub fn err(msg: impl std::fmt::Display) -> Value {
    serde_json::json!({ "ok": false, "error": msg.to_string() })
}
