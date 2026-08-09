mod client;
mod daemon;
mod protocol;
mod semantic;
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
    about = "Pheromone — a distributed event bus for agent ecosystems (prototype: tiers 1-2, local bus)"
)]
struct Cli {
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
    /// Emit an event onto the bus
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
    /// Daemon control
    Daemon {
        #[command(subcommand)]
        cmd: DaemonCmd,
    },
    /// Run an ecosystem tap (feeds the local bus)
    Tap {
        #[command(subcommand)]
        cmd: TapCmd,
    },
}

#[derive(Subcommand)]
enum TapCmd {
    /// Follow the Honeybee ledger and emit `hive.<type>` events
    Hive {
        /// Backlog lookback for the first attach (e.g. 15m); default: live only
        #[arg(long, default_value = "1s")]
        since: String,
    },
}

#[derive(Subcommand)]
enum DaemonCmd {
    /// Run pherd in the foreground
    Run,
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
            let response = client::call(
                &paths,
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
            let response = client::call(
                &paths,
                &Request::When {
                    string: subscription,
                    options,
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
        Cmd::Ls { json } => {
            let response = client::call(&paths, &Request::Ls)?;
            let subs = response["subs"].as_array().cloned().unwrap_or_default();
            if json {
                println!("{}", serde_json::to_string_pretty(&subs)?);
            } else if subs.is_empty() {
                println!("no subscriptions");
            } else {
                for s in subs {
                    println!(
                        "{}  [{} deliveries]  {}",
                        s["id"].as_str().unwrap_or("?"),
                        s["deliveries"],
                        s["string"].as_str().unwrap_or("?")
                    );
                }
            }
        }
        Cmd::Rm { id } => {
            let response = client::call(&paths, &Request::Rm { id: id.clone() })?;
            if response["removed"].as_bool() == Some(true) {
                println!("removed {id}");
            } else {
                bail!("no subscription '{id}'");
            }
        }
        Cmd::Tail { after, subject } => {
            client::tail(&paths, &Request::Tail { after, subject }, |line| {
                println!("{line}");
                Ok(())
            })?;
        }
        Cmd::Why { delivery_id } => {
            let response = client::call(&paths, &Request::Why { delivery_id })?;
            println!("{}", serde_json::to_string_pretty(&response["delivery"])?);
        }
        Cmd::WhyNot { sub_id, event_id } => {
            let response = client::call(
                &paths,
                &Request::WhyNot {
                    sub: sub_id,
                    event: event_id,
                },
            )?;
            println!("{}", serde_json::to_string_pretty(&response["report"])?);
        }
        Cmd::Status => {
            let response = client::call(&paths, &Request::Status)?;
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        Cmd::Daemon {
            cmd: DaemonCmd::Run,
        } => {
            daemon::run(paths)?;
        }
        Cmd::Tap {
            cmd: TapCmd::Hive { since },
        } => {
            tap::run_hive_tap(&paths, &since)?;
        }
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
