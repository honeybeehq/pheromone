//! `pher init` — first-run bootstrap and environment doctor. Creates the
//! state directory, then reports what works, what's missing, and the exact
//! next command for each gap. Read-only apart from `mkdir`; safe to re-run.

use std::process::Command;

use crate::protocol::Request;
use crate::store::Paths;

fn have_cli(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn ok(msg: &str) {
    println!("  ok  {msg}");
}

fn gap(msg: &str) {
    println!("  --  {msg}");
}

pub fn run(paths: &Paths) -> anyhow::Result<()> {
    paths.ensure()?;
    std::fs::create_dir_all(paths.home.join("log"))?;
    println!("pheromone home: {}\n", paths.home.display());

    // Daemon + service.
    let daemon_up = match crate::client::call(paths, &Request::Status) {
        Ok(s) => {
            ok(&format!(
                "daemon running — node '{}', {} subscription(s)",
                s["node"].as_str().unwrap_or("?"),
                s["subscriptions"]
            ));
            true
        }
        Err(_) => {
            gap("daemon not running        → pher daemon install   (or: pher daemon run)");
            false
        }
    };
    if crate::service::plan(paths)
        .map(|p| p.unit_path.exists())
        .unwrap_or(false)
    {
        ok("service installed (starts at login, restarts on crash)");
    } else {
        gap("service not installed     → pher daemon install");
    }

    // Ecosystem sinks/taps: missing CLIs mean those sinks fail at delivery.
    for (cli, why) in [
        ("hive", "then buz / then hive / pher tap hive"),
        ("hermes", "then hermes"),
        ("pol", "then pol"),
    ] {
        if have_cli(cli) {
            ok(&format!("{cli} CLI on PATH ({why})"));
        } else {
            gap(&format!("{cli} CLI not found — '{why}' sinks will fail"));
        }
    }

    // Tier 3: model cache is per-home; first meaning subscription downloads.
    let models_ready = std::fs::read_dir(paths.models())
        .map(|mut d| d.next().is_some())
        .unwrap_or(false);
    if std::env::var("PHER_EMBED").as_deref() == Ok("off") {
        gap("meaning tier disabled (PHER_EMBED=off)");
    } else if models_ready {
        ok("meaning tier ready (embedding model cached, offline from here)");
    } else {
        gap("meaning model not cached — downloads (~30MB, once) on the first 'meaning' subscription");
    }

    // Tier 4: provider keys.
    match crate::judge::resolve_config() {
        Ok(cfg) => ok(&format!(
            "judge tier ready — {} via {:?}",
            cfg.model, cfg.provider
        )),
        Err(_) => gap(
            "judge tier unconfigured — set ANTHROPIC_API_KEY or OPENAI_API_KEY \
             (and optionally PHER_JUDGE_MODEL) to enable 'judge' subscriptions",
        ),
    }

    // Ingress.
    match std::env::var("PHER_HTTP") {
        Ok(addr) => ok(&format!("http ingress configured on {addr}")),
        Err(_) => {
            gap("http ingress off — set PHER_HTTP=127.0.0.1:4870 for webhooks/metrics/cross-node")
        }
    }

    if daemon_up {
        println!("\ntry it:");
        println!("  pher listen 'on demo.*'                                # shell 1");
        println!("  pher emit demo.hello --payload '{{\"n\": 1}}'            # shell 2");
    }
    Ok(())
}
