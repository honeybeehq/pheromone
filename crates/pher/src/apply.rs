//! `pher apply` — declarative config (pheromone.toml). The file is the
//! desired state; apply reconciles the target daemon against it and reports
//! every action. Identity is the `name`: named subscriptions/conditions are
//! managed by the file, unnamed ones (ad-hoc `pher when`) are never touched.
//! With `--prune`, named things on the daemon that are absent from the file
//! are removed — the file owns the namespace.
//!
//! ```toml
//! [[subscription]]
//! name = "ci-failures"
//! when = 'on ci.* where payload.conclusion == "failure" then buz operator'
//!
//! [[condition]]
//! name = "p95_high"
//! metric = "p95_latency"
//! gt = 800
//! hold = "5m"
//! labels = { env = "prod" }
//!
//! [[node]]
//! name = "metal1"
//! url = "http://metal1:4870"
//! token-env = "PHER_HTTP_TOKEN_METAL1"
//! ```

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, Context};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::client::{self, Target};
use crate::protocol::Request;
use crate::store::Paths;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default, rename = "subscription")]
    pub subscriptions: Vec<SubEntry>,
    #[serde(default, rename = "condition")]
    pub conditions: Vec<CondEntry>,
    #[serde(default, rename = "node")]
    pub nodes: Vec<NodeCfg>,
    #[serde(default, rename = "bridge")]
    pub bridges: Vec<BridgeEntry>,
    #[serde(default, rename = "grant")]
    pub grants: Vec<GrantEntry>,
}

/// Pull composition: a durable filtered listen against an upstream bus,
/// re-ingested locally. `from` names a [[node]].
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeEntry {
    pub name: String,
    pub from: String,
    pub sub: String,
    /// Upstream cursor name; default bridge:<name>@<target node>.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// A named bearer token bounded by subscription-language filters.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantEntry {
    pub name: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default, rename = "token-env")]
    pub token_env: Option<String>,
    /// What the token may consume, e.g. 'on team.** where payload.vis != "private"'
    #[serde(default)]
    pub allow: Option<String>,
    /// What the token may publish, e.g. 'on team.anna.**'
    #[serde(default)]
    pub emit: Option<String>,
}

fn fingerprint(token: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(token.as_bytes()))[..12].to_string()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubEntry {
    pub name: String,
    pub when: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CondEntry {
    pub name: String,
    pub metric: String,
    #[serde(default)]
    pub gt: Option<f64>,
    #[serde(default)]
    pub lt: Option<f64>,
    #[serde(default)]
    pub ge: Option<f64>,
    #[serde(default)]
    pub le: Option<f64>,
    #[serde(default = "default_hold")]
    pub hold: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

fn default_hold() -> String {
    "0s".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeCfg {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub token: Option<String>,
    /// Env var holding the token — keeps secrets out of the checked-in file.
    #[serde(default, rename = "token-env")]
    pub token_env: Option<String>,
}

impl CondEntry {
    fn op_and_threshold(&self) -> anyhow::Result<(&'static str, f64)> {
        let set: Vec<(&'static str, f64)> = [
            ("gt", self.gt),
            ("lt", self.lt),
            ("ge", self.ge),
            ("le", self.le),
        ]
        .into_iter()
        .filter_map(|(op, v)| v.map(|t| (op, t)))
        .collect();
        match set.as_slice() {
            [one] => Ok(*one),
            [] => bail!("condition '{}': one of gt/lt/ge/le is required", self.name),
            _ => bail!("condition '{}': exactly one of gt/lt/ge/le", self.name),
        }
    }
}

fn find_config(explicit: Option<&str>, paths: &Paths) -> anyhow::Result<PathBuf> {
    if let Some(f) = explicit {
        return Ok(PathBuf::from(f));
    }
    let cwd = PathBuf::from("pheromone.toml");
    if cwd.exists() {
        return Ok(cwd);
    }
    let home = paths.home.join("pheromone.toml");
    if home.exists() {
        return Ok(home);
    }
    bail!(
        "no pheromone.toml here or in {} — pass a path",
        paths.home.display()
    );
}

fn op_sym_to_name(sym: &str) -> &str {
    match sym {
        ">" => "gt",
        "<" => "lt",
        ">=" => "ge",
        "<=" => "le",
        other => other,
    }
}

fn dur_secs(text: &str) -> Option<u64> {
    pher_core::Dur::parse(text).ok().map(|d| d.secs())
}

pub fn run(
    paths: &Paths,
    target: &Target,
    file: Option<&str>,
    prune: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
    let path = find_config(file, paths)?;
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("cannot read {}", path.display()))?;
    let config: Config = toml::from_str(&text)
        .with_context(|| format!("{} is not a valid pheromone.toml", path.display()))?;
    println!("applying {}", path.display());

    // Validate everything before changing anything: all-or-nothing intent.
    let mut desired_subs: Vec<(String, String)> = Vec::new(); // (name, canon)
    for entry in &config.subscriptions {
        let sub = pher_core::Subscription::parse(&entry.when)
            .map_err(|e| anyhow::anyhow!("subscription '{}': {e}", entry.name))?;
        if sub.then.sink == pher_core::Sink::Stream {
            bail!(
                "subscription '{}': 'then stream' is connection-scoped and cannot be \
                 declared in config",
                entry.name
            );
        }
        desired_subs.push((entry.name.clone(), sub.canon()));
    }
    for entry in &config.conditions {
        entry.op_and_threshold()?;
        if dur_secs(&entry.hold).is_none() {
            bail!("condition '{}': bad hold '{}'", entry.name, entry.hold);
        }
    }
    {
        let mut seen = std::collections::HashSet::new();
        for (name, _) in &desired_subs {
            if !seen.insert(name.clone()) {
                bail!("duplicate subscription name '{name}' in config");
            }
        }
    }

    let act = |line: String| {
        if dry_run {
            println!("  would {line}");
        } else {
            println!("  {line}");
        }
    };

    // -- nodes: local registry (this machine's view of the mesh) ------------
    let mut nodes = client::load_nodes(paths);
    if !config.nodes.is_empty() {
        let mut changed = false;
        for n in &config.nodes {
            let token = match (&n.token, &n.token_env) {
                (Some(t), _) => Some(t.clone()),
                (None, Some(var)) => match std::env::var(var) {
                    Ok(v) if !v.is_empty() => Some(v),
                    _ => {
                        eprintln!(
                            "  warning: node '{}': token-env {var} is unset — storing no token",
                            n.name
                        );
                        None
                    }
                },
                (None, None) => None,
            };
            let entry = client::NodeEntry {
                name: n.name.clone(),
                url: n.url.trim_end_matches('/').to_string(),
                token,
            };
            match nodes.iter_mut().find(|e| e.name == n.name) {
                Some(existing) if existing.url == entry.url && existing.token == entry.token => {
                    println!("  node {}: unchanged", n.name);
                }
                Some(existing) => {
                    act(format!("node {}: updated → {}", n.name, entry.url));
                    *existing = entry;
                    changed = true;
                }
                None => {
                    act(format!("node {}: added → {}", n.name, entry.url));
                    nodes.push(entry);
                    changed = true;
                }
            }
        }
        if changed && !dry_run {
            client::save_nodes(paths, &nodes)?;
        }
    }

    // -- subscriptions: reconcile by name ------------------------------------
    let ls = client::call_target(target, &Request::Ls)?;
    let existing: HashMap<String, (String, String)> = ls["subs"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|s| {
            let name = s.get("name")?.as_str()?.to_string();
            let id = s["id"].as_str()?.to_string();
            let string = s["string"].as_str()?.to_string();
            Some((name, (id, string)))
        })
        .collect();

    for (name, canon) in &desired_subs {
        match existing.get(name) {
            Some((_, current)) if current == canon => {
                println!("  sub {name}: unchanged");
            }
            Some((id, _)) => {
                act(format!("sub {name}: updated"));
                if !dry_run {
                    client::call_target(target, &Request::Rm { id: id.clone() })?;
                    register(target, name, canon)?;
                }
            }
            None => {
                act(format!("sub {name}: created"));
                if !dry_run {
                    register(target, name, canon)?;
                }
            }
        }
    }
    if prune {
        let desired_names: std::collections::HashSet<&str> =
            desired_subs.iter().map(|(n, _)| n.as_str()).collect();
        for (name, (id, _)) in &existing {
            if !desired_names.contains(name.as_str()) {
                act(format!("sub {name}: pruned ({id})"));
                if !dry_run {
                    client::call_target(target, &Request::Rm { id: id.clone() })?;
                }
            }
        }
    }

    // -- conditions: reconcile by name ---------------------------------------
    let cls = client::call_target(target, &Request::ConditionLs)?;
    let existing_conds: HashMap<String, Value> = cls["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|c| Some((c.get("name")?.as_str()?.to_string(), c)))
        .collect();

    for entry in &config.conditions {
        let (op, threshold) = entry.op_and_threshold()?;
        let same = existing_conds.get(&entry.name).is_some_and(|c| {
            c["metric"].as_str() == Some(entry.metric.as_str())
                && c["op"].as_str().map(op_sym_to_name) == Some(op)
                && c["threshold"].as_f64() == Some(threshold)
                && c["hold"].as_str().and_then(dur_secs) == dur_secs(&entry.hold)
                && c["labels"]
                    .as_object()
                    .map(|m| {
                        m.len() == entry.labels.len()
                            && entry
                                .labels
                                .iter()
                                .all(|(k, v)| m.get(k).and_then(|x| x.as_str()) == Some(v.as_str()))
                    })
                    .unwrap_or(entry.labels.is_empty())
        });
        if same {
            println!("  condition {}: unchanged", entry.name);
            continue;
        }
        let existed = existing_conds.contains_key(&entry.name);
        act(format!(
            "condition {}: {}",
            entry.name,
            if existed { "updated" } else { "created" }
        ));
        if !dry_run {
            if existed {
                client::call_target(
                    target,
                    &Request::ConditionRm {
                        name: entry.name.clone(),
                    },
                )?;
            }
            let def = json!({
                "name": entry.name,
                "metric": entry.metric,
                "op": op,
                "threshold": threshold,
                "hold": entry.hold,
                "labels": entry.labels,
            });
            client::call_target(target, &Request::ConditionAdd { def })?;
        }
    }
    if prune {
        let desired: std::collections::HashSet<&str> =
            config.conditions.iter().map(|c| c.name.as_str()).collect();
        for name in existing_conds.keys() {
            if !desired.contains(name.as_str()) {
                act(format!("condition {name}: pruned"));
                if !dry_run {
                    client::call_target(target, &Request::ConditionRm { name: name.clone() })?;
                }
            }
        }
    }

    // -- bridges: reconcile by name -------------------------------------------
    if !config.bridges.is_empty() || prune {
        let bls = client::call_target(target, &Request::BridgeLs)?;
        let existing_bridges: HashMap<String, Value> = bls["bridges"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|b| Some((b.get("name")?.as_str()?.to_string(), b)))
            .collect();
        for entry in &config.bridges {
            let node = nodes
                .iter()
                .find(|n| n.name == entry.from)
                .with_context(|| {
                    format!(
                        "bridge '{}': unknown node '{}' — declare it in [[node]]",
                        entry.name, entry.from
                    )
                })?;
            let cursor = entry.cursor.clone().unwrap_or_default();
            let same = existing_bridges.get(&entry.name).is_some_and(|b| {
                b["url"].as_str() == Some(node.url.as_str())
                    && b["sub"].as_str() == Some(entry.sub.as_str())
                    && (cursor.is_empty() || b["cursor"].as_str() == Some(cursor.as_str()))
                    && b["authed"].as_bool() == Some(node.token.is_some())
            });
            if same {
                println!("  bridge {}: unchanged", entry.name);
                continue;
            }
            let existed = existing_bridges.contains_key(&entry.name);
            act(format!(
                "bridge {}: {} ← {}",
                entry.name,
                if existed { "updated" } else { "created" },
                node.url
            ));
            if !dry_run {
                client::call_target(
                    target,
                    &Request::BridgeAdd {
                        def: json!({
                            "name": entry.name,
                            "url": node.url,
                            "token": node.token,
                            "sub": entry.sub,
                            "cursor": cursor,
                        }),
                    },
                )?;
            }
        }
        if prune {
            let desired: std::collections::HashSet<&str> =
                config.bridges.iter().map(|b| b.name.as_str()).collect();
            for name in existing_bridges.keys() {
                if !desired.contains(name.as_str()) {
                    act(format!("bridge {name}: pruned"));
                    if !dry_run {
                        client::call_target(target, &Request::BridgeRm { name: name.clone() })?;
                    }
                }
            }
        }
    }

    // -- grants: reconcile by name --------------------------------------------
    if !config.grants.is_empty() || prune {
        let gls = client::call_target(target, &Request::GrantLs)?;
        let existing_grants: HashMap<String, Value> = gls["grants"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|g| Some((g.get("name")?.as_str()?.to_string(), g)))
            .collect();
        for entry in &config.grants {
            let token = match (&entry.token, &entry.token_env) {
                (Some(t), _) => t.clone(),
                (None, Some(var)) => std::env::var(var)
                    .ok()
                    .filter(|v| !v.is_empty())
                    .with_context(|| format!("grant '{}': token-env {var} is unset", entry.name))?,
                (None, None) => bail!("grant '{}': token or token-env required", entry.name),
            };
            let same = existing_grants.get(&entry.name).is_some_and(|g| {
                g["tokenFingerprint"].as_str() == Some(fingerprint(&token).as_str())
                    && g["allow"].as_str() == entry.allow.as_deref()
                    && g["emit"].as_str() == entry.emit.as_deref()
            });
            if same {
                println!("  grant {}: unchanged", entry.name);
                continue;
            }
            let existed = existing_grants.contains_key(&entry.name);
            act(format!(
                "grant {}: {}",
                entry.name,
                if existed { "updated" } else { "created" }
            ));
            if !dry_run {
                client::call_target(
                    target,
                    &Request::GrantSet {
                        def: json!({
                            "name": entry.name,
                            "token": token,
                            "allow": entry.allow,
                            "emit": entry.emit,
                        }),
                    },
                )?;
            }
        }
        if prune {
            let desired: std::collections::HashSet<&str> =
                config.grants.iter().map(|g| g.name.as_str()).collect();
            for name in existing_grants.keys() {
                if !desired.contains(name.as_str()) {
                    act(format!("grant {name}: pruned"));
                    if !dry_run {
                        client::call_target(target, &Request::GrantRm { name: name.clone() })?;
                    }
                }
            }
        }
    }

    if dry_run {
        println!("dry run: nothing changed");
    }
    Ok(())
}

fn register(target: &Target, name: &str, canon: &str) -> anyhow::Result<()> {
    let response = client::call_target(
        target,
        &Request::When {
            string: canon.to_string(),
            options: Vec::new(),
            name: Some(name.to_string()),
        },
    )?;
    for w in response["warnings"].as_array().into_iter().flatten() {
        eprintln!("  warning ({name}): {}", w.as_str().unwrap_or_default());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_documented_shape() {
        let cfg: Config = toml::from_str(
            r#"
            [[subscription]]
            name = "ci-failures"
            when = 'on ci.* where payload.conclusion == "failure" then buz operator'

            [[condition]]
            name = "p95_high"
            metric = "p95_latency"
            gt = 800
            hold = "5m"
            labels = { env = "prod" }

            [[node]]
            name = "metal1"
            url = "http://metal1:4870"
            token-env = "PHER_HTTP_TOKEN_METAL1"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.subscriptions.len(), 1);
        assert_eq!(cfg.conditions[0].op_and_threshold().unwrap(), ("gt", 800.0));
        assert_eq!(
            cfg.nodes[0].token_env.as_deref(),
            Some("PHER_HTTP_TOKEN_METAL1")
        );
    }

    #[test]
    fn parses_bridges_and_grants() {
        let cfg: Config = toml::from_str(
            r#"
            [[bridge]]
            name = "company-ci"
            from = "company"
            sub = "on ci.**"

            [[grant]]
            name = "team"
            token-env = "PHER_TOKEN_TEAM"
            allow = 'on ci.**, deploy.*'
            emit = 'on team.oslo.**'
            "#,
        )
        .unwrap();
        assert_eq!(cfg.bridges[0].from, "company");
        assert_eq!(cfg.bridges[0].cursor, None);
        assert_eq!(cfg.grants[0].allow.as_deref(), Some("on ci.**, deploy.*"));
        // Unknown fields are config typos, not silent no-ops.
        assert!(toml::from_str::<Config>("[[grant]]\nname='x'\nalow='on a'").is_err());
    }

    #[test]
    fn rejects_ambiguous_ops_and_unknown_fields() {
        let cond = CondEntry {
            name: "x".into(),
            metric: "m".into(),
            gt: Some(1.0),
            lt: Some(2.0),
            ge: None,
            le: None,
            hold: "0s".into(),
            labels: HashMap::new(),
        };
        assert!(cond.op_and_threshold().is_err());
        assert!(toml::from_str::<Config>("[[subscription]]\nname='a'\nwehn='x'").is_err());
    }
}
