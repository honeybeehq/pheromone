mod apply;
mod bridge;
mod client;
mod conditions;
mod connector;
mod daemon;
mod db;
mod http;
mod init;
mod judge;
mod protocol;
mod semantic;
mod service;
mod store;
mod tap;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use serde_json::Value;

use pher_core::matcher::{evaluate, Outcome};
use pher_core::{Envelope, Subscription};
use protocol::{PartialEvent, Request};
use store::Paths;

#[derive(Parser)]
#[command(
    name = "pher",
    version,
    about = "Pheromone — event trails for agent ecosystems"
)]
struct Cli {
    /// Target a remote node: a name from `pher node add`, or a URL
    #[arg(long, global = true)]
    node: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Parse a subscription string into canonical JSON
    Parse {
        /// Subscription string, e.g. 'on hive.seal where payload.status == "blocked" then cmd echo hit'
        subscription: String,
        /// Print the canonical string form instead of JSON
        #[arg(long)]
        canon: bool,
    },
    /// Convert canonical JSON (file, inline string, or '-' for stdin) to the canonical string form
    Fmt { json: String },
    /// Dry-run a subscription against a bag of events (JSON array or JSONL file of envelopes)
    Test {
        subscription: String,
        /// Events file; each event needs at least {"subject": ...}
        #[arg(long)]
        against: String,
        /// Only print matches
        #[arg(long)]
        matches_only: bool,
    },
    /// Emit an event onto the trail
    Emit {
        subject: String,
        /// JSON payload (default null)
        #[arg(long)]
        payload: Option<String>,
        #[arg(long = "type")]
        event_type: Option<String>,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        correlation: Option<String>,
    },
    /// Register a standing subscription
    When {
        subscription: String,
        /// Lifetime: evaporate after a duration, e.g. --for 30m
        #[arg(long = "for")]
        ttl: Option<String>,
        /// Lifetime: lease to an agent, e.g. --while CL.6308
        #[arg(long = "while")]
        lessee: Option<String>,
        /// Debounce window, e.g. --every 30m
        #[arg(long)]
        every: Option<String>,
        /// Batch window, e.g. --batch 1h
        #[arg(long)]
        batch: Option<String>,
        /// Replay lookback before going live, e.g. --since 24h
        #[arg(long)]
        since: Option<String>,
        /// Max deliveries, then retire
        #[arg(long)]
        limit: Option<u64>,
        /// Stable name for declarative reconciliation (pher apply)
        #[arg(long)]
        name: Option<String>,
    },
    /// Apply a declarative config (pheromone.toml): reconcile subscriptions,
    /// conditions, and nodes against the target daemon
    Apply {
        /// Config file (default: ./pheromone.toml, else ~/.pheromone/pheromone.toml)
        file: Option<String>,
        /// Remove named subscriptions/conditions on the daemon that are not in the file
        #[arg(long)]
        prune: bool,
        /// Report what would change without changing it
        #[arg(long)]
        dry_run: bool,
    },
    /// List registered subscriptions
    Ls {
        #[arg(long)]
        json: bool,
    },
    /// Remove a subscription
    Rm { id: String },
    /// Stream events (backlog from --after, then live)
    Tail {
        /// Resume after this sequence number (cursor)
        #[arg(long)]
        after: Option<u64>,
        /// Subject pattern filter, e.g. 'hive.**'
        #[arg(long)]
        subject: Option<String>,
    },
    /// Register a connection-scoped subscription and stream its deliveries
    /// (the subscription is removed when you disconnect)
    Listen {
        /// Subscription text; `then stream` is appended if there is no then-clause
        subscription: String,
        /// Resume: replay matches from log seq > N, then go live
        #[arg(long)]
        after: Option<u64>,
        /// Named hub-side cursor: resume from its committed position and
        /// commit each delivery as it is printed
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Inspect or remove named consumer cursors
    Cursor {
        #[command(subcommand)]
        cmd: CursorCmd,
    },
    /// Connect a SaaS vendor: extract its events onto the trail
    Connect {
        #[command(subcommand)]
        cmd: ConnectCmd,
    },
    /// Follow an upstream trail: a filtered stream, re-ingested locally
    /// (sugar for `pher bridge add`)
    Follow {
        /// Upstream: a node name from `pher node add`, or a URL
        from: String,
        /// Subscription text ('then stream' implied), e.g. 'on ci.**'
        #[arg(long)]
        sub: String,
        /// Bridge name (default: derived from the upstream name)
        #[arg(long)]
        name: Option<String>,
        /// Upstream bearer token (defaults to the node's registered token)
        #[arg(long)]
        token: Option<String>,
        /// Upstream cursor name (default: bridge:<name>@<node>)
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Manage bridges (follow filtered streams from upstream trails)
    Bridge {
        #[command(subcommand)]
        cmd: BridgeCmd,
    },
    /// Inspect or remove token grants (set them via pher apply)
    Grant {
        #[command(subcommand)]
        cmd: GrantCmd,
    },
    /// Show the full match record for a delivery
    Why {
        #[arg(name = "delivery-id")]
        delivery_id: String,
    },
    /// Replay one event through one subscription; report the first rejecting tier
    WhyNot {
        #[arg(name = "sub-id")]
        sub_id: String,
        #[arg(name = "event-id")]
        event_id: String,
    },
    /// Daemon status
    Status,
    /// Open the live console (served by the daemon's HTTP ingress)
    Ui,
    /// First-run bootstrap: create the state dir and report environment gaps
    Init,
    /// Daemon control
    Daemon {
        #[command(subcommand)]
        cmd: DaemonCmd,
    },
    /// Run an ecosystem tap (feeds the local trail)
    Tap {
        #[command(subcommand)]
        cmd: TapCmd,
    },
    /// Manage metric conditions (datapoints in, transitions out)
    Condition {
        #[command(subcommand)]
        cmd: ConditionCmd,
    },
    /// Manage remote nodes (cross-node targets for --node)
    Node {
        #[command(subcommand)]
        cmd: NodeCmd,
    },
}

#[derive(Subcommand)]
enum ConnectCmd {
    /// Add/replace a connector, e.g.: pher connect add sentry --token env:SENTRY_TOKEN --param org=acme
    Add {
        /// Manifest name from the catalog (see: pher connect catalog)
        manifest: String,
        /// Instance name (default: the manifest name)
        #[arg(long)]
        name: Option<String>,
        /// Token source: literal, env:VAR, or cmd:<command> (secret managers
        /// like hem/1Password plug in via cmd:, e.g. cmd:hem get x --field y)
        #[arg(long)]
        token: Option<String>,
        /// Manifest params, repeatable: --param org=acme
        #[arg(long = "param")]
        params: Vec<String>,
    },
    /// List configured connectors (tokens shown as fingerprints)
    Ls,
    /// Remove a connector
    Rm { name: String },
    /// List available connector manifests (built-in + ~/.pheromone/connectors)
    Catalog,
}

#[derive(Subcommand)]
enum BridgeCmd {
    /// Add/replace a bridge, e.g.: pher bridge add company-ci --from company --sub 'on ci.**'
    Add {
        name: String,
        /// Upstream: a node name from `pher node add`, or a URL
        #[arg(long)]
        from: String,
        /// Subscription text ('then stream' implied)
        #[arg(long)]
        sub: String,
        /// Upstream bearer token (defaults to the node's registered token)
        #[arg(long)]
        token: Option<String>,
        /// Upstream cursor name (default: bridge:<name>@<node>)
        #[arg(long)]
        cursor: Option<String>,
    },
    /// List bridges
    Ls,
    /// Remove a bridge
    Rm { name: String },
}

#[derive(Subcommand)]
enum GrantCmd {
    /// List grants (tokens shown as fingerprints only)
    Ls {
        #[arg(long)]
        json: bool,
    },
    /// Remove a grant
    Rm { name: String },
}

#[derive(Subcommand)]
enum CursorCmd {
    /// List named cursors and the log head
    Ls {
        #[arg(long)]
        json: bool,
    },
    /// Remove a cursor
    Rm { name: String },
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Register a node, e.g.: pher node add studio --url http://studio:4870 --token s3cret
    Add {
        name: String,
        #[arg(long)]
        url: String,
        #[arg(long)]
        token: Option<String>,
    },
    /// List registered nodes
    Ls,
    /// Remove a node
    Rm { name: String },
}

#[derive(Subcommand)]
enum ConditionCmd {
    /// Add a condition, e.g.: pher condition add p95_high --metric p95_latency --gt 800 --for 5m --label env=prod
    Add {
        name: String,
        #[arg(long)]
        metric: String,
        #[arg(long)]
        gt: Option<f64>,
        #[arg(long)]
        lt: Option<f64>,
        #[arg(long)]
        ge: Option<f64>,
        #[arg(long)]
        le: Option<f64>,
        /// Predicate must hold this long before entering (e.g. 5m)
        #[arg(long = "for", default_value = "0s")]
        hold: String,
        /// Required labels, repeatable: --label env=prod
        #[arg(long = "label")]
        labels: Vec<String>,
    },
    /// List conditions with live state
    Ls {
        #[arg(long)]
        json: bool,
    },
    /// Remove a condition
    Rm { name: String },
}

#[derive(Subcommand)]
enum TapCmd {
    /// Follow the Honeybee ledger and emit `hive.<type>` events
    Hive {
        /// Backlog lookback for the first attach (e.g. 15m); default: live only
        #[arg(long, default_value = "1s")]
        since: String,
        /// Ledger type prefixes to drop at the tap edge (repeatable),
        /// e.g. --exclude state.verified for liveness-probe heartbeats
        #[arg(long = "exclude")]
        excludes: Vec<String>,
    },
}

#[derive(Subcommand)]
enum DaemonCmd {
    /// Run pherd in the foreground
    Run,
    /// Install pherd as a user service (launchd/systemd): starts now and at login
    Install,
    /// Stop and remove the pherd user service
    Uninstall,
    /// Show service install state and live daemon status
    Status,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("pher: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let paths = Paths::resolve();
    let target = client::resolve_target(&paths, cli.node.as_deref())?;
    let remote = !matches!(target, client::Target::Local(_));
    match cli.cmd {
        Cmd::Parse {
            subscription,
            canon,
        } => {
            let sub = Subscription::parse(&subscription)?;
            if canon {
                println!("{}", sub.canon());
            } else {
                println!("{}", serde_json::to_string_pretty(&sub.to_json())?);
            }
        }
        Cmd::Fmt { json } => {
            let text = if json == "-" {
                std::io::read_to_string(std::io::stdin())?
            } else if std::path::Path::new(&json).exists() {
                std::fs::read_to_string(&json)?
            } else {
                json
            };
            let value: Value = serde_json::from_str(&text).context("input is not valid JSON")?;
            let sub = Subscription::from_json(&value)?;
            println!("{}", sub.canon());
        }
        Cmd::Test {
            subscription,
            against,
            matches_only,
        } => {
            let sub = Subscription::parse(&subscription)?;
            let events = load_events(&against)?;
            if events.is_empty() {
                bail!("no events found in {against}");
            }
            let mut matched = 0usize;
            for (i, event) in events.iter().enumerate() {
                let eval = evaluate("TEST", &sub, event, None);
                let label = if event.id.is_empty() {
                    format!("#{i}")
                } else {
                    event.id.clone()
                };
                match &eval.outcome {
                    Outcome::Matched => {
                        matched += 1;
                        let block = serde_json::to_string(&eval.match_block)?;
                        println!("MATCH   {label} {} {block}", event.subject);
                    }
                    Outcome::PendingSemantic { pending } => {
                        if !matches_only {
                            println!(
                                "PENDING {label} {} passed tiers 1-2; {} not evaluated in this build",
                                event.subject,
                                pending.join("+")
                            );
                        }
                    }
                    Outcome::Rejected { tier, reason } => {
                        if !matches_only {
                            println!("reject  {label} {} [{tier}] {reason}", event.subject);
                        }
                    }
                }
            }
            eprintln!("-- {matched}/{} events matched", events.len());
        }
        Cmd::Emit {
            subject,
            payload,
            event_type,
            source,
            correlation,
        } => {
            let payload = payload
                .map(|p| serde_json::from_str(&p).context("--payload is not valid JSON"))
                .transpose()?;
            let response = client::call_target(
                &target,
                &Request::Emit {
                    event: PartialEvent {
                        subject,
                        payload,
                        event_type,
                        source,
                        correlation,
                    },
                },
            )?;
            println!(
                "emitted {} (seq {}, {} delivery/ies)",
                response["id"].as_str().unwrap_or("?"),
                response["seq"],
                response["deliveries"]
            );
        }
        Cmd::When {
            subscription,
            ttl,
            lessee,
            every,
            batch,
            since,
            limit,
            name,
        } => {
            let mut options: Vec<String> = Vec::new();
            if let Some(d) = ttl {
                options.extend(["for".into(), d]);
            }
            if let Some(b) = lessee {
                options.extend(["while".into(), b, "alive".into()]);
            }
            if let Some(d) = every {
                options.extend(["every".into(), d]);
            }
            if let Some(d) = batch {
                options.extend(["batch".into(), d]);
            }
            if let Some(d) = since {
                options.extend(["since".into(), d]);
            }
            if let Some(n) = limit {
                options.extend(["limit".into(), n.to_string()]);
            }
            let response = client::call_target(
                &target,
                &Request::When {
                    string: subscription,
                    options,
                    name,
                },
            )?;
            println!(
                "registered {}: {}",
                response["id"].as_str().unwrap_or("?"),
                response["canonical"].as_str().unwrap_or("?")
            );
            if let Some(n) = response["replayedDeliveries"].as_u64() {
                if n > 0 {
                    println!("replayed {n} delivery/ies from the backlog");
                }
            }
            if let Some(warnings) = response["warnings"].as_array() {
                for w in warnings {
                    eprintln!("warning: {}", w.as_str().unwrap_or_default());
                }
            }
        }
        Cmd::Apply {
            file,
            prune,
            dry_run,
        } => {
            apply::run(&paths, &target, file.as_deref(), prune, dry_run)?;
        }
        Cmd::Ls { json } => {
            let response = client::call_target(&target, &Request::Ls)?;
            let subs = response["subs"].as_array().cloned().unwrap_or_default();
            if json {
                println!("{}", serde_json::to_string_pretty(&subs)?);
            } else if subs.is_empty() {
                println!("no subscriptions");
            } else {
                for s in subs {
                    let name = s["name"]
                        .as_str()
                        .map(|n| format!(" ({n})"))
                        .unwrap_or_default();
                    println!(
                        "{}{name}  [{} deliveries]  {}",
                        s["id"].as_str().unwrap_or("?"),
                        s["deliveries"],
                        s["string"].as_str().unwrap_or("?")
                    );
                }
            }
        }
        Cmd::Rm { id } => {
            let response = client::call_target(&target, &Request::Rm { id: id.clone() })?;
            if response["removed"].as_bool() == Some(true) {
                println!("removed {id}");
            } else {
                bail!("no subscription '{id}'");
            }
        }
        Cmd::Tail { after, subject } => {
            if remote {
                bail!("tail is a streaming op — not supported over --node yet");
            }
            client::tail(&paths, &Request::Tail { after, subject }, |line| {
                println!("{line}");
                Ok(())
            })?;
        }
        Cmd::Listen {
            subscription,
            after,
            cursor,
        } => {
            // Sugar: a bare `on ... where ...` gets `then stream` appended.
            let text = if Subscription::parse(&subscription).is_ok() {
                subscription
            } else {
                format!("{subscription} then stream")
            };
            Subscription::parse(&text)?; // surface parse errors before connecting
            let client_name = format!("pher-listen-{}@{}", std::process::id(), daemon::hostname());
            // A delivery counts as processed once printed; commit it then.
            let commit_cursor = cursor.clone();
            let commit_target = &target;
            let print = move |line: Value| -> anyhow::Result<()> {
                if let Some(canonical) = line.get("canonical").and_then(|c| c.as_str()) {
                    eprintln!(
                        "listening as {} — {canonical}",
                        line["id"].as_str().unwrap_or("?")
                    );
                    if let (Some(from), Some(replayed)) =
                        (line.get("resumedFrom"), line.get("replayed"))
                    {
                        eprintln!("resumed from seq {from}: {replayed} replayed delivery/ies");
                    }
                    if let Some(gap) = line.get("gapExpired") {
                        eprintln!(
                            "warning: {gap} event(s) in the gap already evaporated (retention)"
                        );
                    }
                    for w in line["warnings"].as_array().into_iter().flatten() {
                        eprintln!("warning: {}", w.as_str().unwrap_or_default());
                    }
                } else {
                    println!("{line}");
                    if let (Some(name), Some(seq)) = (
                        commit_cursor.as_deref(),
                        line.get("seq").and_then(|s| s.as_u64()),
                    ) {
                        let _ = client::call_target(
                            commit_target,
                            &Request::CursorCommit {
                                name: name.to_string(),
                                seq,
                            },
                        );
                    }
                }
                Ok(())
            };
            match &target {
                client::Target::Local(_) => {
                    let request = Request::Listen {
                        string: text,
                        options: Vec::new(),
                        client: Some(client_name),
                        after,
                        cursor: cursor.clone(),
                    };
                    client::tail(&paths, &request, print)?;
                }
                client::Target::Remote { url, token } => {
                    let mut body = serde_json::json!({ "string": text, "client": client_name });
                    if let Some(a) = after {
                        body["after"] = serde_json::json!(a);
                    }
                    if let Some(c) = &cursor {
                        body["cursor"] = serde_json::json!(c);
                    }
                    client::listen_remote(url, token.as_deref(), &body, print)?;
                }
            }
        }
        Cmd::Connect { cmd } => match cmd {
            ConnectCmd::Add {
                manifest,
                name,
                token,
                params,
            } => {
                // Token resolution is strictly CLI-side: the daemon stores the
                // resolved value (0600) and never shells out for secrets.
                let token = match token {
                    Some(spec) => connector::resolve_token(&spec)?,
                    None => String::new(),
                };
                let mut param_map = serde_json::Map::new();
                for p in params {
                    let (k, v) = p.split_once('=').context("--param expects key=value")?;
                    param_map.insert(k.to_string(), Value::String(v.to_string()));
                }
                let instance = name.unwrap_or_else(|| manifest.clone());
                let response = client::call_target(
                    &target,
                    &Request::ConnectorAdd {
                        def: serde_json::json!({
                            "name": instance,
                            "use": manifest,
                            "token": token,
                            "params": param_map,
                        }),
                    },
                )?;
                println!(
                    "connector {} → extracting via '{}' (events land under its vendor subjects)",
                    response["name"].as_str().unwrap_or("?"),
                    response["use"].as_str().unwrap_or("?")
                );
            }
            ConnectCmd::Ls => {
                let response = client::call_target(&target, &Request::ConnectorLs)?;
                let connectors = response["connectors"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                if connectors.is_empty() {
                    println!("no connectors (see: pher connect catalog)");
                }
                for c in connectors {
                    println!(
                        "{}  use={}  [{}]  {}",
                        c["name"].as_str().unwrap_or("?"),
                        c["use"].as_str().unwrap_or("?"),
                        c["tokenFingerprint"].as_str().unwrap_or("-"),
                        serde_json::to_string(&c["params"]).unwrap_or_default(),
                    );
                }
            }
            ConnectCmd::Rm { name } => {
                let response =
                    client::call_target(&target, &Request::ConnectorRm { name: name.clone() })?;
                if response["removed"].as_bool() == Some(true) {
                    println!("removed {name} (worker winds down)");
                } else {
                    bail!("no connector '{name}'");
                }
            }
            ConnectCmd::Catalog => {
                for (name, manifest, origin) in connector::catalog(&paths) {
                    let params: Vec<String> = manifest.params.keys().cloned().collect();
                    println!(
                        "{name}  [{origin}]  {}{}",
                        manifest.description,
                        if params.is_empty() {
                            String::new()
                        } else {
                            format!("  (params: {})", params.join(", "))
                        }
                    );
                }
                println!(
                    "\nadd your own: drop a manifest in {}",
                    paths.connectors_dir().display()
                );
            }
        },
        Cmd::Follow {
            from,
            sub,
            name,
            token,
            cursor,
        } => {
            // "Follow the company trail": the bridge name defaults to the
            // upstream's name (sanitized when it's a URL).
            let name = name.unwrap_or_else(|| {
                from.trim_start_matches("http://")
                    .trim_start_matches("https://")
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
                    .collect::<String>()
                    .trim_matches('-')
                    .to_string()
            });
            follow_trail(&target, &paths, name, from, sub, token, cursor)?;
        }
        Cmd::Bridge { cmd } => match cmd {
            BridgeCmd::Add {
                name,
                from,
                sub,
                token,
                cursor,
            } => {
                follow_trail(&target, &paths, name, from, sub, token, cursor)?;
            }
            BridgeCmd::Ls => {
                let response = client::call_target(&target, &Request::BridgeLs)?;
                let bridges = response["bridges"].as_array().cloned().unwrap_or_default();
                if bridges.is_empty() {
                    println!("no bridges");
                }
                for b in bridges {
                    println!(
                        "{}  ← {}  '{}'  cursor={}",
                        b["name"].as_str().unwrap_or("?"),
                        b["url"].as_str().unwrap_or("?"),
                        b["sub"].as_str().unwrap_or("?"),
                        b["cursor"].as_str().unwrap_or("?"),
                    );
                }
            }
            BridgeCmd::Rm { name } => {
                let response =
                    client::call_target(&target, &Request::BridgeRm { name: name.clone() })?;
                if response["removed"].as_bool() == Some(true) {
                    println!("removed {name} (winds down within ~15s)");
                } else {
                    bail!("no bridge '{name}'");
                }
            }
        },
        Cmd::Grant { cmd } => match cmd {
            GrantCmd::Ls { json } => {
                let response = client::call_target(&target, &Request::GrantLs)?;
                let grants = response["grants"].as_array().cloned().unwrap_or_default();
                if json {
                    println!("{}", serde_json::to_string_pretty(&grants)?);
                } else if grants.is_empty() {
                    println!("no grants");
                } else {
                    for g in grants {
                        println!(
                            "{}  [{}]  allow: {}  emit: {}",
                            g["name"].as_str().unwrap_or("?"),
                            g["tokenFingerprint"].as_str().unwrap_or("?"),
                            g["allow"].as_str().unwrap_or("-"),
                            g["emit"].as_str().unwrap_or("-"),
                        );
                    }
                }
            }
            GrantCmd::Rm { name } => {
                let response =
                    client::call_target(&target, &Request::GrantRm { name: name.clone() })?;
                if response["removed"].as_bool() == Some(true) {
                    println!("removed {name}");
                } else {
                    bail!("no grant '{name}'");
                }
            }
        },
        Cmd::Cursor { cmd } => match cmd {
            CursorCmd::Ls { json } => {
                let response = client::call_target(&target, &Request::CursorLs)?;
                let cursors = response["cursors"].as_array().cloned().unwrap_or_default();
                if json {
                    println!("{}", serde_json::to_string_pretty(&response)?);
                } else if cursors.is_empty() {
                    println!("no cursors (log head: seq {})", response["head"]);
                } else {
                    println!("log head: seq {}", response["head"]);
                    for c in cursors {
                        println!(
                            "{}  seq {}  committed {}",
                            c["name"].as_str().unwrap_or("?"),
                            c["seq"],
                            c["committedAt"].as_str().unwrap_or("?")
                        );
                    }
                }
            }
            CursorCmd::Rm { name } => {
                let response =
                    client::call_target(&target, &Request::CursorRm { name: name.clone() })?;
                if response["removed"].as_bool() == Some(true) {
                    println!("removed {name}");
                } else {
                    bail!("no cursor '{name}'");
                }
            }
        },
        Cmd::Why { delivery_id } => {
            let response = client::call_target(&target, &Request::Why { delivery_id })?;
            println!("{}", serde_json::to_string_pretty(&response["delivery"])?);
        }
        Cmd::WhyNot { sub_id, event_id } => {
            let response = client::call_target(
                &target,
                &Request::WhyNot {
                    sub: sub_id,
                    event: event_id,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&response["report"])?);
        }
        Cmd::Status => {
            let response = client::call_target(&target, &Request::Status)?;
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        Cmd::Ui => {
            let url = match &target {
                client::Target::Remote { url, .. } => format!("{url}/ui"),
                client::Target::Local(_) => match std::env::var("PHER_HTTP") {
                    Ok(addr) if !addr.is_empty() => format!("http://{addr}/ui"),
                    _ => {
                        // No ingress configured: check whether one is running
                        // anyway (daemon env differs from shell env).
                        let probe = "http://127.0.0.1:4870";
                        if ureq::get(&format!("{probe}/healthz"))
                            .timeout(std::time::Duration::from_millis(500))
                            .call()
                            .is_ok()
                        {
                            format!("{probe}/ui")
                        } else {
                            bail!(
                                "no HTTP ingress detected — run the daemon with PHER_HTTP set:\n  \
                                 PHER_HTTP=127.0.0.1:4870 pher daemon run\n\
                                 (or reinstall the service: PHER_HTTP=127.0.0.1:4870 pher daemon install)"
                            );
                        }
                    }
                },
            };
            println!("console: {url}");
            #[cfg(target_os = "macos")]
            let _ = std::process::Command::new("open").arg(&url).spawn();
            #[cfg(target_os = "linux")]
            let _ = std::process::Command::new("xdg-open").arg(&url).spawn();
        }
        Cmd::Init => {
            if remote {
                bail!("init is local-only");
            }
            init::run(&paths)?;
        }
        Cmd::Daemon { cmd } => {
            if remote {
                bail!("the daemon always runs locally");
            }
            match cmd {
                DaemonCmd::Run => daemon::run(paths)?,
                DaemonCmd::Install => service::install(&paths)?,
                DaemonCmd::Uninstall => service::uninstall(&paths)?,
                DaemonCmd::Status => service::status(&paths)?,
            }
        }
        Cmd::Tap {
            cmd: TapCmd::Hive { since, excludes },
        } => {
            tap::run_hive_tap(&target, &paths, &since, &excludes)?;
        }
        Cmd::Node { cmd } => match cmd {
            NodeCmd::Add { name, url, token } => {
                let mut nodes = client::load_nodes(&paths);
                nodes.retain(|n| n.name != name);
                nodes.push(client::NodeEntry {
                    name: name.clone(),
                    url,
                    token,
                });
                client::save_nodes(&paths, &nodes)?;
                println!("node '{name}' registered");
            }
            NodeCmd::Ls => {
                let nodes = client::load_nodes(&paths);
                if nodes.is_empty() {
                    println!("no nodes registered");
                }
                for n in nodes {
                    println!(
                        "{}  {}  [{}]",
                        n.name,
                        n.url,
                        if n.token.is_some() { "token" } else { "open" }
                    );
                }
            }
            NodeCmd::Rm { name } => {
                let mut nodes = client::load_nodes(&paths);
                let before = nodes.len();
                nodes.retain(|n| n.name != name);
                if nodes.len() == before {
                    bail!("no node '{name}'");
                }
                client::save_nodes(&paths, &nodes)?;
                println!("removed {name}");
            }
        },
        Cmd::Condition { cmd } => match cmd {
            ConditionCmd::Add {
                name,
                metric,
                gt,
                lt,
                ge,
                le,
                hold,
                labels,
            } => {
                let ops = [("gt", gt), ("lt", lt), ("ge", ge), ("le", le)];
                let set: Vec<_> = ops.iter().filter(|(_, v)| v.is_some()).collect();
                let (op, threshold) = match set.as_slice() {
                    [(op, Some(t))] => (op.to_string(), *t),
                    [] => bail!("one of --gt/--lt/--ge/--le is required"),
                    _ => bail!("exactly one of --gt/--lt/--ge/--le, not several"),
                };
                let mut label_map = serde_json::Map::new();
                for l in labels {
                    let (k, v) = l.split_once('=').context("--label expects key=value")?;
                    label_map.insert(k.to_string(), Value::String(v.to_string()));
                }
                let def = serde_json::json!({
                    "name": name,
                    "metric": metric,
                    "op": op,
                    "threshold": threshold,
                    "hold": hold,
                    "labels": label_map,
                });
                client::call_target(&target, &Request::ConditionAdd { def })?;
                println!("condition '{name}' added");
            }
            ConditionCmd::Ls { json } => {
                let response = client::call_target(&target, &Request::ConditionLs)?;
                let conditions = response["conditions"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                if json {
                    println!("{}", serde_json::to_string_pretty(&conditions)?);
                } else if conditions.is_empty() {
                    println!("no conditions");
                } else {
                    for c in conditions {
                        println!(
                            "{}  {} {} {} for {}  [{}] last={}",
                            c["name"].as_str().unwrap_or("?"),
                            c["metric"].as_str().unwrap_or("?"),
                            c["op"].as_str().unwrap_or("?"),
                            c["threshold"],
                            c["hold"].as_str().unwrap_or("?"),
                            if c["active"].as_bool() == Some(true) {
                                "ACTIVE"
                            } else {
                                "clear"
                            },
                            c["lastValue"],
                        );
                    }
                }
            }
            ConditionCmd::Rm { name } => {
                let response =
                    client::call_target(&target, &Request::ConditionRm { name: name.clone() })?;
                if response["removed"].as_bool() == Some(true) {
                    println!("removed {name}");
                } else {
                    bail!("no condition '{name}'");
                }
            }
        },
    }
    Ok(())
}

/// Load events from a JSON array file or JSONL file. Partial envelopes are
/// filled with test defaults so `pher test` works on hand-written fixtures.
fn load_events(path: &str) -> anyhow::Result<Vec<Envelope>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
    let values: Vec<Value> = if text.trim_start().starts_with('[') {
        serde_json::from_str(&text)?
    } else {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?
    };
    values.into_iter().map(partial_envelope).collect()
}

fn partial_envelope(mut v: Value) -> anyhow::Result<Envelope> {
    let obj = v
        .as_object_mut()
        .context("each event must be a JSON object")?;
    // Accept a bare `{seq, event}` tail line too.
    if let Some(inner) = obj.get("event").cloned() {
        if obj.contains_key("seq") {
            return partial_envelope(inner);
        }
    }
    let subject = obj
        .get("subject")
        .and_then(|s| s.as_str())
        .context("event is missing 'subject'")?
        .to_string();
    let defaults = [
        ("id", Value::String(String::new())),
        ("ts", Value::String("2026-01-01T00:00:00Z".into())),
        ("node", Value::String("test-node".into())),
        ("source", Value::String("test".into())),
        ("type", Value::String(subject.clone())),
    ];
    for (key, default) in defaults {
        obj.entry(key).or_insert(default);
    }
    Ok(serde_json::from_value(v)?)
}

/// Register (or replace) a bridge that follows an upstream trail. Shared by
/// `pher follow` and `pher bridge add`.
fn follow_trail(
    target: &client::Target,
    paths: &Paths,
    name: String,
    from: String,
    sub: String,
    token: Option<String>,
    cursor: Option<String>,
) -> anyhow::Result<()> {
    // Resolve the upstream from the node registry, or take a URL.
    let (url, node_token) = if from.starts_with("http://") || from.starts_with("https://") {
        (from.trim_end_matches('/').to_string(), None)
    } else {
        let nodes = client::load_nodes(paths);
        let node = nodes
            .iter()
            .find(|n| n.name == from)
            .with_context(|| format!("unknown node '{from}' — pher node add"))?;
        (node.url.clone(), node.token.clone())
    };
    let response = client::call_target(
        target,
        &Request::BridgeAdd {
            def: serde_json::json!({
                "name": name,
                "url": url,
                "token": token.or(node_token),
                "sub": sub,
                "cursor": cursor.unwrap_or_default(),
            }),
        },
    )?;
    println!(
        "following {url} as '{name}' (cursor {})",
        response["cursor"].as_str().unwrap_or("?")
    );
    Ok(())
}
