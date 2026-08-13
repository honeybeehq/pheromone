//! `pher daemon install|uninstall|status` — run pherd under the OS service
//! manager (launchd on macOS, a systemd user unit on Linux) so it survives
//! reboots and crashes without anyone babysitting a terminal.
//!
//! The service definition captures the *install-time* environment: the
//! resolved `PHEROMONE_HOME` plus every `PHER_*` variable currently set.
//! That makes `PHER_HTTP=… PHER_HTTP_TOKEN=… pher daemon install` the way to
//! install a hub, and it means secrets land in the (0600) unit file — which
//! we tell the user about.

use std::io::Write as _;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{bail, Context};

use crate::store::Paths;

pub const LABEL: &str = "dev.pher.pherd";

pub struct ServicePlan {
    /// Path the unit/plist is written to.
    pub unit_path: PathBuf,
    /// Rendered unit file contents.
    pub unit_text: String,
    /// Log file the daemon's stdout/stderr goes to.
    pub log_path: PathBuf,
    /// Env vars captured into the unit (names only, for reporting).
    pub captured_env: Vec<String>,
}

fn captured_env(paths: &Paths) -> Vec<(String, String)> {
    let mut vars: Vec<(String, String)> = vec![(
        "PHEROMONE_HOME".to_string(),
        paths.home.display().to_string(),
    )];
    let mut pher: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("PHER_"))
        .collect();
    pher.sort();
    vars.extend(pher);
    vars
}

fn home_dir() -> anyhow::Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
}

fn exe_path() -> anyhow::Result<PathBuf> {
    let exe = std::env::current_exe().context("cannot resolve the pher binary path")?;
    Ok(exe.canonicalize().unwrap_or(exe))
}

/// Render the platform's service definition without touching the system.
pub fn plan(paths: &Paths) -> anyhow::Result<ServicePlan> {
    let exe = exe_path()?;
    let log_dir = paths.home.join("log");
    let log_path = log_dir.join("pherd.log");
    let env = captured_env(paths);
    let captured: Vec<String> = env.iter().map(|(k, _)| k.clone()).collect();

    if cfg!(target_os = "macos") {
        let unit_path = home_dir()?
            .join("Library/LaunchAgents")
            .join(format!("{LABEL}.plist"));
        let env_xml: String = env
            .iter()
            .map(|(k, v)| {
                format!(
                    "    <key>{}</key>\n    <string>{}</string>\n",
                    xml_escape(k),
                    xml_escape(v)
                )
            })
            .collect();
        let unit_text = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{exe}</string>
    <string>daemon</string>
    <string>run</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>5</integer>
  <key>EnvironmentVariables</key>
  <dict>
{env_xml}  </dict>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
</dict>
</plist>
"#,
            exe = xml_escape(&exe.display().to_string()),
            log = xml_escape(&log_path.display().to_string()),
        );
        Ok(ServicePlan {
            unit_path,
            unit_text,
            log_path,
            captured_env: captured,
        })
    } else if cfg!(target_os = "linux") {
        let unit_path = home_dir()?
            .join(".config/systemd/user")
            .join("pherd.service");
        let env_lines: String = env
            .iter()
            .map(|(k, v)| format!("Environment=\"{k}={v}\"\n"))
            .collect();
        let unit_text = format!(
            r#"[Unit]
Description=Pheromone event bus daemon (pherd)

[Service]
ExecStart={exe} daemon run
Restart=always
RestartSec=5
{env_lines}StandardOutput=append:{log}
StandardError=append:{log}

[Install]
WantedBy=default.target
"#,
            exe = exe.display(),
            log = log_path.display(),
        );
        Ok(ServicePlan {
            unit_path,
            unit_text,
            log_path,
            captured_env: captured,
        })
    } else {
        bail!("daemon install supports macOS (launchd) and Linux (systemd) only");
    }
}

fn run_quiet(cmd: &mut Command) -> anyhow::Result<bool> {
    Ok(cmd
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .with_context(|| format!("failed to run {:?}", cmd.get_program()))?
        .success())
}

fn gui_domain() -> String {
    // launchctl user domains are keyed by uid; `id -u` avoids libc bindings.
    let uid = Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "501".to_string());
    format!("gui/{uid}")
}

pub fn install(paths: &Paths) -> anyhow::Result<()> {
    let plan = plan(paths)?;
    let exe = exe_path()?;
    if exe.components().any(|c| c.as_os_str() == "target") {
        eprintln!(
            "warning: installing a build-tree binary ({}) — a `cargo build` can break the \
             service; prefer an installed binary",
            exe.display()
        );
    }

    std::fs::create_dir_all(plan.log_path.parent().unwrap())?;
    std::fs::create_dir_all(plan.unit_path.parent().unwrap())?;
    // Unit may hold tokens (PHER_HTTP_TOKEN etc.): owner-only before content.
    let mut f = std::fs::File::create(&plan.unit_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    f.write_all(plan.unit_text.as_bytes())?;
    drop(f);

    if cfg!(target_os = "macos") {
        let domain = gui_domain();
        // Re-installs: tear the old instance down first; ignore "not loaded".
        let _ =
            run_quiet(Command::new("launchctl").args(["bootout", &format!("{domain}/{LABEL}")]));
        // bootout is asynchronous: an immediate bootstrap can race the old
        // instance's teardown and fail. Retry briefly.
        let plist = plan.unit_path.to_str().context("non-utf8 plist path")?;
        let mut loaded = false;
        for _ in 0..10 {
            if run_quiet(Command::new("launchctl").args(["bootstrap", &domain, plist]))? {
                loaded = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        if !loaded {
            bail!(
                "launchctl bootstrap failed — inspect with: launchctl print {domain}/{LABEL}; \
                 log: {}",
                plan.log_path.display()
            );
        }
    } else {
        if !run_quiet(Command::new("systemctl").args(["--user", "daemon-reload"]))? {
            bail!("systemctl --user daemon-reload failed");
        }
        if !run_quiet(Command::new("systemctl").args(["--user", "enable", "--now", "pherd"]))? {
            bail!("systemctl --user enable --now pherd failed — journalctl --user -u pherd");
        }
    }

    println!("installed {}", plan.unit_path.display());
    println!("captured env: {}", plan.captured_env.join(", "));
    if plan.captured_env.iter().any(|k| k.contains("TOKEN")) {
        println!("note: tokens are stored in the unit file (mode 0600)");
    }
    println!("logs: {}", plan.log_path.display());
    Ok(())
}

pub fn uninstall(paths: &Paths) -> anyhow::Result<()> {
    let plan = plan(paths)?;
    if cfg!(target_os = "macos") {
        let _ = run_quiet(
            Command::new("launchctl").args(["bootout", &format!("{}/{LABEL}", gui_domain())]),
        );
    } else {
        let _ = run_quiet(Command::new("systemctl").args(["--user", "disable", "--now", "pherd"]));
    }
    if plan.unit_path.exists() {
        std::fs::remove_file(&plan.unit_path)?;
        println!("removed {}", plan.unit_path.display());
    } else {
        println!("no service installed ({} absent)", plan.unit_path.display());
    }
    Ok(())
}

/// Install + runtime state, as human-readable lines.
pub fn status(paths: &Paths) -> anyhow::Result<()> {
    let plan = plan(paths)?;
    if plan.unit_path.exists() {
        println!("service:  installed ({})", plan.unit_path.display());
    } else {
        println!("service:  not installed");
    }
    match crate::client::call(paths, &crate::protocol::Request::Status) {
        Ok(s) => println!(
            "daemon:   running — node '{}', {} subscription(s), {} listener(s)",
            s["node"].as_str().unwrap_or("?"),
            s["subscriptions"],
            s["listeners"]
        ),
        Err(_) => println!(
            "daemon:   not running (no socket at {})",
            paths.sock().display()
        ),
    }
    println!("logs:     {}", plan.log_path.display());
    Ok(())
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_paths() -> Paths {
        Paths {
            home: std::path::PathBuf::from("/tmp/pher-svc-test"),
        }
    }

    #[test]
    fn plan_renders_platform_unit() {
        let plan = plan(&temp_paths()).unwrap();
        assert!(plan.captured_env.contains(&"PHEROMONE_HOME".to_string()));
        assert!(plan.unit_text.contains("daemon"));
        assert!(plan.unit_text.contains("run"));
        assert!(plan
            .unit_text
            .contains(&plan.log_path.display().to_string()));
        if cfg!(target_os = "macos") {
            assert!(plan.unit_text.contains("<key>KeepAlive</key>"));
            assert!(plan
                .unit_path
                .ends_with("Library/LaunchAgents/dev.pher.pherd.plist"));
            assert!(plan
                .unit_text
                .contains("<string>/tmp/pher-svc-test</string>"));
        } else if cfg!(target_os = "linux") {
            assert!(plan.unit_text.contains("Restart=always"));
            assert!(plan.unit_text.contains("PHEROMONE_HOME=/tmp/pher-svc-test"));
        }
    }

    #[test]
    fn xml_escaping() {
        assert_eq!(xml_escape("a<b>&c"), "a&lt;b&gt;&amp;c");
    }
}
