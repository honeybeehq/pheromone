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
        /// Stable name for declarative reconciliation (`pher apply`); unique
        /// among live subscriptions.
        #[serde(default)]
        name: Option<String>,
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
    /// Register a connection-scoped subscription (`then stream`) and stream
    /// its deliveries down this connection until either side goes away.
    Listen {
        string: String,
        #[serde(default)]
        options: Vec<String>,
        /// Listener name recorded as the lease lessee (`while <client> alive`).
        #[serde(default)]
        client: Option<String>,
        /// Resume: replay matches from log events with seq > after, then live.
        #[serde(default)]
        after: Option<u64>,
        /// Named hub-side cursor: resume from its committed seq (see
        /// CursorCommit); takes effect when `after` is not given.
        #[serde(default)]
        cursor: Option<String>,
    },
    /// Advance a named cursor to seq (monotonic: max wins). Consumers commit
    /// after processing, so redelivery-on-crash errs toward at-least-once.
    CursorCommit {
        name: String,
        seq: u64,
    },
    CursorLs,
    CursorRm {
        name: String,
    },
    /// Upsert a bridge: a durable pull from an upstream bus — its /listen
    /// filtered by `sub`, re-ingested locally with envelope identity
    /// preserved. `url`/`token` are resolved from the node registry by the
    /// caller (`pher apply` / `pher bridge add`).
    BridgeAdd {
        def: Value,
    },
    BridgeLs,
    BridgeRm {
        name: String,
    },
    /// Upsert a grant: a named bearer token whose read/write surface is a
    /// pair of subscription-language filters (tiers 1-2 only).
    GrantSet {
        def: Value,
    },
    GrantLs,
    GrantRm {
        name: String,
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
    ConditionAdd {
        def: Value,
    },
    ConditionLs,
    ConditionRm {
        name: String,
    },
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
