use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;

use anyhow::{bail, Context};
use serde_json::Value;

use crate::protocol::Request;
use crate::store::Paths;

/// Where a CLI command is directed: the local daemon's unix socket, or a
/// remote daemon's HTTP /rpc (cross-node).
pub enum Target {
    Local(Paths),
    Remote { url: String, token: Option<String> },
}

/// A named remote node in ~/.pheromone/nodes.json.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct NodeEntry {
    pub name: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub token: Option<String>,
}

pub fn nodes_path(paths: &Paths) -> std::path::PathBuf {
    paths.home.join("nodes.json")
}

pub fn load_nodes(paths: &Paths) -> Vec<NodeEntry> {
    std::fs::read_to_string(nodes_path(paths))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

pub fn save_nodes(paths: &Paths, nodes: &[NodeEntry]) -> anyhow::Result<()> {
    paths.ensure()?;
    crate::store::write_json_atomic(&nodes_path(paths), &serde_json::to_value(nodes)?)
}

/// `--node` accepts a registered name or a bare URL for ad-hoc targets.
pub fn resolve_target(paths: &Paths, node: Option<&str>) -> anyhow::Result<Target> {
    let Some(node) = node else {
        return Ok(Target::Local(paths.clone()));
    };
    if node.starts_with("http://") || node.starts_with("https://") {
        return Ok(Target::Remote {
            url: node.trim_end_matches('/').to_string(),
            token: std::env::var("PHER_HTTP_TOKEN").ok(),
        });
    }
    let nodes = load_nodes(paths);
    let entry = nodes
        .iter()
        .find(|n| n.name == node)
        .with_context(|| format!("unknown node '{node}' — add it with `pher node add`"))?;
    Ok(Target::Remote {
        url: entry.url.trim_end_matches('/').to_string(),
        token: entry.token.clone(),
    })
}

/// Dispatch a request to wherever the target lives.
pub fn call_target(target: &Target, request: &Request) -> anyhow::Result<Value> {
    match target {
        Target::Local(paths) => call(paths, request),
        Target::Remote { url, token } => {
            let mut req = ureq::post(&format!("{url}/rpc"))
                .timeout(std::time::Duration::from_secs(15))
                .set("content-type", "application/json");
            if let Some(t) = token {
                req = req.set("authorization", &format!("Bearer {t}"));
            }
            let response = req
                .send_string(&serde_json::to_string(request)?)
                .map_err(|e| anyhow::anyhow!("remote node unreachable at {url}: {e}"))?;
            let text = response.into_string()?;
            let value: Value = serde_json::from_str(&text)
                .map_err(|e| anyhow::anyhow!("remote response not JSON: {e}"))?;
            if value.get("ok").and_then(|v| v.as_bool()) != Some(true) {
                bail!(
                    "{}",
                    value
                        .get("error")
                        .and_then(|e| e.as_str())
                        .unwrap_or("unknown remote error")
                );
            }
            Ok(value)
        }
    }
}

pub fn connect(paths: &Paths) -> anyhow::Result<UnixStream> {
    UnixStream::connect(paths.sock()).with_context(|| {
        format!(
            "pherd is not running (no socket at {}) — start it with `pher daemon run`",
            paths.sock().display()
        )
    })
}

/// A persistent daemon connection: many request/response cycles on one
/// socket. Taps hold one of these for their whole life.
pub struct Conn {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
}

impl Conn {
    pub fn connect(paths: &Paths) -> anyhow::Result<Conn> {
        let stream = connect(paths)?;
        let reader = BufReader::new(stream.try_clone()?);
        Ok(Conn { stream, reader })
    }

    pub fn call(&mut self, request: &Request) -> anyhow::Result<Value> {
        let mut line = serde_json::to_string(request)?;
        line.push('\n');
        self.stream.write_all(line.as_bytes())?;
        self.stream.flush()?;
        let mut response = String::new();
        if self.reader.read_line(&mut response)? == 0 {
            bail!("pherd closed the connection");
        }
        let value: Value = serde_json::from_str(response.trim())?;
        if value.get("ok").and_then(|v| v.as_bool()) != Some(true) {
            bail!(
                "{}",
                value
                    .get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("unknown daemon error")
            );
        }
        Ok(value)
    }
}

/// Send one request, read one JSON-line response. Errors on `ok: false`.
pub fn call(paths: &Paths, request: &Request) -> anyhow::Result<Value> {
    let mut stream = connect(paths)?;
    let mut line = serde_json::to_string(request)?;
    line.push('\n');
    stream.write_all(line.as_bytes())?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    if reader.read_line(&mut response)? == 0 {
        bail!("pherd closed the connection without responding");
    }
    let value: Value = serde_json::from_str(response.trim())?;
    if value.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        bail!(
            "{}",
            value
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown daemon error")
        );
    }
    Ok(value)
}

/// Stream a remote node's POST /listen (chunked NDJSON): ack line first,
/// then one line per delivery; blank heartbeat lines are skipped. Returns
/// when the server ends the stream (subscription removed) or on error.
pub fn listen_remote(
    url: &str,
    token: Option<&str>,
    body: &Value,
    mut on_line: impl FnMut(Value) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut req = ureq::post(&format!("{url}/listen")).set("content-type", "application/json");
    if let Some(t) = token {
        req = req.set("authorization", &format!("Bearer {t}"));
    }
    let response = req.send_string(&body.to_string()).map_err(|e| match e {
        ureq::Error::Status(code, resp) => {
            let text = resp.into_string().unwrap_or_default();
            let msg = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v.get("error").and_then(|x| x.as_str()).map(String::from))
                .unwrap_or(text);
            anyhow::anyhow!("remote listen failed ({code}): {msg}")
        }
        other => anyhow::anyhow!("remote node unreachable at {url}: {other}"),
    })?;
    let reader = BufReader::new(response.into_reader());
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue; // heartbeat
        }
        let value: Value = serde_json::from_str(&line)?;
        if value.get("ok").and_then(|v| v.as_bool()) == Some(false) {
            bail!(
                "{}",
                value
                    .get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("unknown remote error")
            );
        }
        on_line(value)?;
    }
    Ok(())
}

/// Open a tail stream; hand each JSON line to `on_line` until EOF/ctrl-c.
pub fn tail(
    paths: &Paths,
    request: &Request,
    mut on_line: impl FnMut(Value) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut stream = connect(paths)?;
    let mut line = serde_json::to_string(request)?;
    line.push('\n');
    stream.write_all(line.as_bytes())?;
    stream.flush()?;
    let reader = BufReader::new(stream);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)?;
        if value.get("ok").and_then(|v| v.as_bool()) == Some(false) {
            bail!(
                "{}",
                value
                    .get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("unknown daemon error")
            );
        }
        on_line(value)?;
    }
    Ok(())
}
