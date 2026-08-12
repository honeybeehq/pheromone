//! HTTP ingress: the push-tap runtime. Three endpoints, one listener:
//!
//! - `POST /webhook/<name>` — generic webhook tap (Sentry, Vercel, PostHog,
//!   GitHub, anything that can POST JSON). HMAC-SHA256 verification when a
//!   secret is configured; events land as `webhook.<name>`.
//! - `POST /emit` — remote emit (other tailnet nodes, scripts, taps).
//! - `POST /metric` — datapoint intake for the condition engine; only
//!   condition transitions reach the bus, never the datapoints themselves.
//!
//! Bind via `PHER_HTTP` (e.g. `127.0.0.1:4870`, or a tailnet address).
//! Binding beyond loopback REQUIRES `PHER_HTTP_TOKEN` — refused otherwise.
//! Webhook secrets: `PHER_WEBHOOK_SECRET` (all hooks) or
//! `PHER_WEBHOOK_SECRET_<NAME>` (per hook, name uppercased).

use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};

use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;

use crate::daemon::State;
use crate::protocol::{PartialEvent, Request};

const MAX_BODY_BYTES: usize = 1 << 20; // 1 MiB

pub(crate) struct HttpConfig {
    pub addr: String,
    pub token: Option<String>,
}

/// Resolve the ingress config from the environment. Loopback binds may run
/// tokenless; anything else must be authenticated.
pub(crate) fn resolve_config() -> anyhow::Result<Option<HttpConfig>> {
    let Some(addr) = std::env::var("PHER_HTTP").ok().filter(|a| !a.is_empty()) else {
        return Ok(None);
    };
    let token = std::env::var("PHER_HTTP_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    let loopback = addr.starts_with("127.") || addr.starts_with("localhost");
    if !loopback && token.is_none() {
        anyhow::bail!(
            "PHER_HTTP={addr} binds beyond loopback; set PHER_HTTP_TOKEN (bearer auth) \
             or bind to 127.0.0.1"
        );
    }
    Ok(Some(HttpConfig { addr, token }))
}

pub(crate) fn start(state: Arc<Mutex<State>>, config: HttpConfig) -> anyhow::Result<()> {
    let server = tiny_http::Server::http(&config.addr)
        .map_err(|e| anyhow::anyhow!("cannot bind http ingress on {}: {e}", config.addr))?;
    eprintln!("pherd http ingress on {}", config.addr);
    std::thread::spawn(move || {
        for request in server.incoming_requests() {
            let state = Arc::clone(&state);
            let token = config.token.clone();
            std::thread::spawn(move || handle(request, state, token));
        }
    });
    Ok(())
}

fn handle(mut request: tiny_http::Request, state: Arc<Mutex<State>>, token: Option<String>) {
    let method = request.method().to_string();
    let url = request.url().to_string();
    let path = url.split('?').next().unwrap_or("").to_string();

    // Read the body up-front (needed for HMAC over exact bytes).
    let mut body = Vec::new();
    let _ = request
        .as_reader()
        .take(MAX_BODY_BYTES as u64 + 1)
        .read_to_end(&mut body);
    if body.len() > MAX_BODY_BYTES {
        respond(
            request,
            413,
            json!({"ok": false, "error": "body too large"}),
        );
        return;
    }

    let header = |name: &str| -> Option<String> {
        request
            .headers()
            .iter()
            .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case(name))
            .map(|h| h.value.as_str().to_string())
    };

    let authed = match &token {
        None => true,
        Some(t) => header("authorization")
            .map(|v| v == format!("Bearer {t}"))
            .unwrap_or(false),
    };

    match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") => respond(request, 200, json!({"ok": true})),
        ("POST", "/rpc") => {
            // The full (non-streaming) protocol over HTTP — remote CLI.
            if !authed {
                return respond(request, 401, json!({"ok": false, "error": "unauthorized"}));
            }
            let Ok(rpc) = serde_json::from_slice::<Request>(&body) else {
                return respond(request, 400, json!({"ok": false, "error": "bad request"}));
            };
            let response = crate::daemon::handle_rpc(rpc, &state);
            respond(request, 200, response);
        }
        ("POST", "/listen") => {
            // Remote code-based subscribers: register a connection-scoped
            // `then stream` subscription and stream deliveries as chunked
            // NDJSON. First line is the ack; blank lines are heartbeats
            // (bounded disconnect detection). Dropping the response tears the
            // subscription down — the lease semantics, over the tailnet.
            if !authed {
                return respond(request, 401, json!({"ok": false, "error": "unauthorized"}));
            }
            let Ok(v) = serde_json::from_slice::<Value>(&body) else {
                return respond(request, 400, json!({"ok": false, "error": "invalid JSON"}));
            };
            let Some(string) = v.get("string").and_then(|s| s.as_str()) else {
                return respond(
                    request,
                    400,
                    json!({"ok": false, "error": "expected {string, options?, client?}"}),
                );
            };
            let options: Vec<String> = v
                .get("options")
                .and_then(|o| o.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let client = v
                .get("client")
                .and_then(|c| c.as_str())
                .unwrap_or("http-listener")
                .to_string();
            let after = v.get("after").and_then(|a| a.as_u64());
            let cursor = v.get("cursor").and_then(|c| c.as_str());
            let (sub_id, ack, rx) = match crate::daemon::attach_listener(
                &state, string, &options, &client, after, cursor,
            ) {
                Ok(attached) => attached,
                Err(e) => return respond(request, 400, e),
            };
            // Hand-rolled response: tiny_http's Response path buffers twice
            // (1KiB BufWriter + the chunked encoder's 8KiB chunk buffer), so
            // nothing would reach the client until the stream ENDS. Writing
            // the status line, headers, and chunk frames ourselves — with a
            // flush per line — is what makes this a live stream.
            let mut writer = request.into_writer();
            let reason = (|| -> &'static str {
                let head = "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\ncache-control: no-store\r\n\r\n";
                if writer.write_all(head.as_bytes()).is_err() || writer.flush().is_err() {
                    return "listener disconnected";
                }
                if write_chunk(&mut writer, &format!("{ack}\n")).is_err() {
                    return "listener disconnected";
                }
                loop {
                    match rx.recv_timeout(std::time::Duration::from_secs(15)) {
                        Ok(line) => {
                            if write_chunk(&mut writer, &format!("{line}\n")).is_err() {
                                return "listener disconnected";
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            // Heartbeat: detects a vanished client within one
                            // interval even when no deliveries flow.
                            if write_chunk(&mut writer, "\n").is_err() {
                                return "listener disconnected";
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            // Sub removed daemon-side (rm/limit/ttl): end the
                            // response properly with the terminal chunk.
                            let _ = writer.write_all(b"0\r\n\r\n");
                            let _ = writer.flush();
                            return "stream ended";
                        }
                    }
                }
            })();
            crate::daemon::detach_listener(&state, &sub_id, reason);
        }
        ("POST", "/deliver") => {
            // Filter-at-source landing zone: a remote node's http sink posts
            // its delivery JSON here; only matches ever cross the wire.
            if !authed {
                return respond(request, 401, json!({"ok": false, "error": "unauthorized"}));
            }
            let Ok(delivery) = serde_json::from_slice::<Value>(&body) else {
                return respond(request, 400, json!({"ok": false, "error": "invalid JSON"}));
            };
            let result = state.lock().unwrap().ingest_forwarded(delivery);
            match result {
                Ok(v) => respond(request, 200, v),
                Err(e) => respond(request, 400, json!({"ok": false, "error": e})),
            }
        }
        ("POST", "/emit") => {
            if !authed {
                return respond(request, 401, json!({"ok": false, "error": "unauthorized"}));
            }
            let Ok(event) = serde_json::from_slice::<PartialEvent>(&body) else {
                return respond(
                    request,
                    400,
                    json!({"ok": false, "error": "body must be a partial event with at least {subject}"}),
                );
            };
            let result = state.lock().unwrap().ingest(event, 0);
            match result {
                Ok(v) => respond(request, 200, v),
                Err(e) => respond(request, 400, json!({"ok": false, "error": e})),
            }
        }
        ("POST", "/metric") => {
            if !authed {
                return respond(request, 401, json!({"ok": false, "error": "unauthorized"}));
            }
            let Ok(v) = serde_json::from_slice::<Value>(&body) else {
                return respond(request, 400, json!({"ok": false, "error": "invalid JSON"}));
            };
            let (Some(name), Some(value)) = (
                v.get("name").and_then(|n| n.as_str()),
                v.get("value").and_then(|x| x.as_f64()),
            ) else {
                return respond(
                    request,
                    400,
                    json!({"ok": false, "error": "expected {name, value, labels?}"}),
                );
            };
            let labels: HashMap<String, String> = v
                .get("labels")
                .and_then(|l| l.as_object())
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, x)| x.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            let transitions = state.lock().unwrap().ingest_metric(name, value, &labels);
            respond(
                request,
                200,
                json!({"ok": true, "transitions": transitions}),
            );
        }
        ("POST", p) if p.starts_with("/webhook/") => {
            let name = sanitize_hook_name(&p["/webhook/".len()..]);
            if name.is_empty() {
                return respond(
                    request,
                    404,
                    json!({"ok": false, "error": "missing hook name"}),
                );
            }
            if let Some(secret) = webhook_secret(&name) {
                let sig = header("x-hub-signature-256")
                    .or_else(|| header("x-signature"))
                    .or_else(|| header("x-pher-signature"));
                if !verify_hmac(&secret, &body, sig.as_deref()) {
                    return respond(
                        request,
                        401,
                        json!({"ok": false, "error": "signature verification failed"}),
                    );
                }
            }
            let payload = serde_json::from_slice::<Value>(&body)
                .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&body).to_string() }));
            let event = PartialEvent {
                subject: format!("webhook.{name}"),
                payload: Some(payload),
                event_type: None,
                source: Some(format!("tap.webhook.{name}")),
                correlation: None,
            };
            let result = state.lock().unwrap().ingest(event, 0);
            match result {
                Ok(v) => respond(request, 200, v),
                Err(e) => respond(request, 400, json!({"ok": false, "error": e})),
            }
        }
        _ => respond(request, 404, json!({"ok": false, "error": "not found"})),
    }
}

/// One HTTP/1.1 chunk frame: size line, payload, CRLF — flushed immediately.
fn write_chunk(writer: &mut impl std::io::Write, data: &str) -> std::io::Result<()> {
    write!(writer, "{:X}\r\n", data.len())?;
    writer.write_all(data.as_bytes())?;
    writer.write_all(b"\r\n")?;
    writer.flush()
}

fn respond(request: tiny_http::Request, code: u16, body: Value) {
    let response = tiny_http::Response::from_string(body.to_string())
        .with_status_code(code)
        .with_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                .expect("static header"),
        );
    let _ = request.respond(response);
}

fn sanitize_hook_name(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn webhook_secret(name: &str) -> Option<String> {
    let specific = format!(
        "PHER_WEBHOOK_SECRET_{}",
        name.to_uppercase().replace('-', "_")
    );
    std::env::var(specific)
        .ok()
        .or_else(|| std::env::var("PHER_WEBHOOK_SECRET").ok())
        .filter(|s| !s.is_empty())
}

/// GitHub-style `sha256=<hex>` (bare hex also accepted). Constant-time via
/// the hmac crate's verify.
fn verify_hmac(secret: &str, body: &[u8], signature: Option<&str>) -> bool {
    let Some(sig) = signature else { return false };
    let hex_part = sig.strip_prefix("sha256=").unwrap_or(sig);
    let Ok(expected) = hex::decode(hex_part.trim()) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_verification() {
        let secret = "topsecret";
        let body = b"{\"hello\": \"world\"}";
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = hex::encode(mac.finalize().into_bytes());

        assert!(verify_hmac(secret, body, Some(&format!("sha256={sig}"))));
        assert!(verify_hmac(secret, body, Some(&sig)));
        assert!(!verify_hmac(secret, body, Some("sha256=deadbeef")));
        assert!(!verify_hmac(secret, body, None));
        assert!(!verify_hmac("wrong", body, Some(&format!("sha256={sig}"))));
    }

    #[test]
    fn hook_names_sanitized() {
        assert_eq!(sanitize_hook_name("Sentry"), "sentry");
        assert_eq!(sanitize_hook_name("my_hook-1"), "my_hook-1");
        assert_eq!(sanitize_hook_name("../etc/passwd"), "etc-passwd");
        assert_eq!(sanitize_hook_name("//"), "");
    }
}
