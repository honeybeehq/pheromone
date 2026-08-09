use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;

use anyhow::{bail, Context};
use serde_json::Value;

use crate::protocol::Request;
use crate::store::Paths;

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
