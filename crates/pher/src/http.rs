//! HTTP ingress: the push-tap runtime. Three endpoints, one listener:
//!
//! - `POST /webhook/<name>` — generic webhook tap (Sentry, Vercel, PostHog,
//!   GitHub, anything that can POST JSON). Every request must carry the
//!   hook's HMAC-SHA256 signature or secret token; a hook without a secret is
//!   closed. Events land as `webhook.<name>`.
//! - `POST /emit` — remote emit (other tailnet nodes, scripts, taps).
//! - `POST /metric` — datapoint intake for the condition engine; only
//!   condition transitions reach the trail, never the datapoints themselves.
//!
//! Bind via `PHER_HTTP` (e.g. `127.0.0.1:4870`, or a tailnet address).
//! Binding beyond loopback REQUIRES `PHER_HTTP_TOKEN` — refused otherwise.
//! Webhook secrets: `[[webhook]]` in pheromone.toml (via `pher apply`),
//! `PHER_WEBHOOK_SECRET_<NAME>` (per hook, name uppercased), or
//! `PHER_WEBHOOK_SECRET` (all hooks). See `webhook.rs`.

use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

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

    // Three auth tiers: the admin token operates the trail; a grant token
    // speaks and listens through its filters; anonymous gets health checks.
    // A tokenless (loopback-only) bind trusts everyone as admin.
    let bearer = header("authorization").and_then(|v| v.strip_prefix("Bearer ").map(String::from));
    let principal = match (&token, &bearer) {
        (None, _) => Principal::Admin,
        (Some(t), Some(b)) if b == t => Principal::Admin,
        (Some(_), Some(b)) => match state.lock().unwrap().grant_for_token(b) {
            Some(name) => Principal::Grant(name),
            None => Principal::Anon,
        },
        (Some(_), None) => Principal::Anon,
    };
    let authed = matches!(principal, Principal::Admin);

    match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") => respond(request, 200, json!({"ok": true})),
        ("GET", "/") | ("GET", "/ui") => {
            // The live console: a single self-contained page served by the
            // daemon itself. Anyone who can reach the port can load the HTML;
            // every API call it makes is auth-checked as usual.
            let response = tiny_http::Response::from_string(UI_HTML)
                .with_status_code(200)
                .with_header(
                    tiny_http::Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"text/html; charset=utf-8"[..],
                    )
                    .expect("static header"),
                );
            let _ = request.respond(response);
        }
        ("POST", "/tail") => {
            // Raw event feed for consoles/debugging: backlog then live, as
            // chunked NDJSON. Unlike /listen this registers no subscription
            // and records no deliveries. Admin-only (it is the firehose).
            if !authed {
                return respond(request, 401, json!({"ok": false, "error": "unauthorized"}));
            }
            let v: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
            let after = v.get("after").and_then(|a| a.as_u64());
            let subject = v.get("subject").and_then(|s| s.as_str()).map(String::from);
            let last = v.get("last").and_then(|l| l.as_u64()).map(|l| l as usize);
            let attached =
                match crate::daemon::attach_tail(&state, after, subject, last.or(Some(100))) {
                    Ok(t) => t,
                    Err(e) => return respond(request, 400, e),
                };
            let mut writer = request.into_writer();
            let head = "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ntransfer-encoding: chunked\r\ncache-control: no-store\r\n\r\n";
            if writer.write_all(head.as_bytes()).is_err() || writer.flush().is_err() {
                return;
            }
            let mut last_sent = after.unwrap_or(0);
            let mut write_ok = true;
            if let Err(e) = crate::daemon::page_tail_backlog(
                &attached.log,
                attached.after,
                attached.last,
                attached.pattern.as_ref(),
                attached.through,
                |seq, event| {
                    if write_chunk(
                        &mut writer,
                        &format!("{}\n", json!({"seq": seq, "event": event})),
                    )
                    .is_err()
                    {
                        write_ok = false;
                        return false;
                    }
                    last_sent = seq;
                    true
                },
            ) {
                let _ = write_chunk(&mut writer, &format!("{}\n", crate::protocol::err(&e)));
                return;
            }
            if !write_ok {
                return;
            }
            loop {
                match attached.rx.recv_timeout(std::time::Duration::from_secs(5)) {
                    Ok(line) => {
                        let seq = serde_json::from_str::<Value>(&line)
                            .ok()
                            .and_then(|v| v.get("seq").and_then(|s| s.as_u64()))
                            .unwrap_or(u64::MAX);
                        if seq <= last_sent {
                            continue; // already sent in the backlog
                        }
                        if write_chunk(&mut writer, &format!("{line}\n")).is_err() {
                            return; // client gone; tails retain() drops us
                        }
                        last_sent = seq;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if write_chunk(&mut writer, "\n").is_err() {
                            return;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        let _ = writer.write_all(b"0\r\n\r\n");
                        let _ = writer.flush();
                        return;
                    }
                }
            }
        }
        ("GET", "/.well-known/pheromone") => {
            // Trail discovery: enough to point a bridge or SDK at, no secrets.
            respond(
                request,
                200,
                json!({
                    "pheromone": true,
                    "version": env!("CARGO_PKG_VERSION"),
                    "auth": if token.is_some() { "bearer" } else { "none" },
                    "surfaces": ["/rpc", "/listen", "/deliver", "/emit", "/metric", "/webhook/<name>"],
                }),
            )
        }
        ("POST", "/rpc") => {
            // The full (non-streaming) protocol over HTTP — remote CLI.
            let Ok(rpc) = serde_json::from_slice::<Request>(&body) else {
                return respond(request, 400, json!({"ok": false, "error": "bad request"}));
            };
            match &principal {
                Principal::Admin => {}
                Principal::Grant(name) => {
                    // Grants speak (filtered emit) and track their position;
                    // operating the trail needs the admin token.
                    match &rpc {
                        Request::Emit { event } => {
                            if let Err(e) = state.lock().unwrap().check_grant_emit(name, event) {
                                return respond(request, 403, json!({"ok": false, "error": e}));
                            }
                        }
                        Request::CursorCommit { .. } | Request::CursorLs => {}
                        _ => {
                            return respond(
                                request,
                                403,
                                json!({"ok": false, "error": "this op requires the admin token"}),
                            );
                        }
                    }
                }
                Principal::Anon => {
                    return respond(request, 401, json!({"ok": false, "error": "unauthorized"}));
                }
            }
            let response = crate::daemon::handle_rpc(rpc, &state);
            respond(request, 200, response);
        }
        ("POST", "/listen") => {
            // Remote code-based subscribers: register a connection-scoped
            // `then stream` subscription and stream deliveries as chunked
            // NDJSON. First line is the ack; blank lines are heartbeats
            // (bounded disconnect detection). Dropping the response tears the
            // subscription down — the lease semantics, over the tailnet.
            // Grant tokens listen through their allow filter (intersection).
            let filter = match &principal {
                Principal::Admin => None,
                Principal::Grant(name) => {
                    let s = state.lock().unwrap();
                    match s.grants.get(name).and_then(|g| g.allow_sub.clone()) {
                        Some(f) => Some(f),
                        None => {
                            drop(s);
                            return respond(
                                request,
                                403,
                                json!({"ok": false, "error": format!("grant '{name}' has no allow filter")}),
                            );
                        }
                    }
                }
                Principal::Anon => {
                    return respond(request, 401, json!({"ok": false, "error": "unauthorized"}));
                }
            };
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
            let attached = match crate::daemon::attach_listener(
                &state, string, &options, &client, after, cursor, filter,
            ) {
                Ok(attached) => attached,
                Err(e) => return respond(request, 400, e),
            };
            let sub_id = attached.id.clone();
            let ack = attached.ack.clone();
            let rx = attached.rx;
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
                let mut last_sent = 0u64;
                if let Some(from) = attached.catchup_from {
                    let mut write_ok = true;
                    match crate::daemon::replay_listen_catchup(
                        &state,
                        &sub_id,
                        &attached.log,
                        from,
                        attached.catchup_through,
                        attached.ts_cutoff.as_deref(),
                        |line| {
                            let seq = serde_json::from_str::<Value>(line)
                                .ok()
                                .and_then(|v| v.get("seq").and_then(|s| s.as_u64()))
                                .unwrap_or(0);
                            if write_chunk(&mut writer, &format!("{line}\n")).is_err() {
                                write_ok = false;
                                return false;
                            }
                            last_sent = last_sent.max(seq);
                            true
                        },
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            let _ = write_chunk(
                                &mut writer,
                                &format!("{}\n", crate::protocol::err(&e)),
                            );
                            return "catch-up failed";
                        }
                    }
                    if !write_ok {
                        return "listener disconnected";
                    }
                }
                loop {
                    match rx.recv_timeout(std::time::Duration::from_secs(5)) {
                        Ok(line) => {
                            let seq = serde_json::from_str::<Value>(&line)
                                .ok()
                                .and_then(|v| v.get("seq").and_then(|s| s.as_u64()))
                                .unwrap_or(u64::MAX);
                            if seq <= last_sent {
                                continue;
                            }
                            if write_chunk(&mut writer, &format!("{line}\n")).is_err() {
                                return "listener disconnected";
                            }
                            last_sent = seq;
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
            let Ok(event) = serde_json::from_slice::<PartialEvent>(&body) else {
                return respond(
                    request,
                    400,
                    json!({"ok": false, "error": "body must be a partial event with at least {subject}"}),
                );
            };
            match &principal {
                Principal::Admin => {}
                Principal::Grant(name) => {
                    if let Err(e) = state.lock().unwrap().check_grant_emit(name, &event) {
                        return respond(request, 403, json!({"ok": false, "error": e}));
                    }
                }
                Principal::Anon => {
                    return respond(request, 401, json!({"ok": false, "error": "unauthorized"}));
                }
            }
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
            let name = crate::webhook::sanitize_name(&p["/webhook/".len()..]);
            if name.is_empty() {
                return respond(
                    request,
                    404,
                    json!({"ok": false, "error": "missing hook name"}),
                );
            }
            let paths = state.lock().unwrap().paths_clone();
            if let Err(rejection) = crate::webhook::authenticate(&paths, &name, &body, header) {
                let message = rejection.message(&name);
                eprintln!("pherd: webhook '{name}' rejected ({})", rejection.status());
                return respond(
                    request,
                    rejection.status(),
                    json!({"ok": false, "error": message}),
                );
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

/// The live console, embedded so the daemon is self-contained.
const UI_HTML: &str = include_str!("ui.html");

enum Principal {
    /// The trail operator (PHER_HTTP_TOKEN, or any caller on a tokenless
    /// loopback bind).
    Admin,
    /// A named grant: emit through its emit filter, listen through its
    /// allow filter, commit cursors. Nothing else.
    Grant(String),
    Anon,
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
