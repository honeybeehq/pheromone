use std::io::Write;
use std::sync::Mutex;

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::protocol::{err, ok, Request};
use crate::store::Paths;

const SIGNATURE_HEADERS: [&str; 5] = [
    "x-hub-signature-256",
    "sentry-hook-signature",
    "linear-signature",
    "x-pher-signature",
    "x-signature",
];
const TOKEN_HEADERS: [&str; 2] = ["x-pher-webhook-token", "x-gitlab-token"];
pub(crate) const MIN_SECRET_LEN: usize = 16;
const GLOBAL_ENV_VAR: &str = "PHER_WEBHOOK_SECRET";

static WRITE_LOCK: Mutex<()> = Mutex::new(());

#[derive(Serialize, Deserialize)]
struct WebhookDef {
    name: String,
    secret: String,
}

struct Secret {
    value: String,
    hook_specific: bool,
}

pub(crate) enum Rejection {
    NotConfigured,
    BadCredentials,
    SecretsUnreadable,
}

impl Rejection {
    pub(crate) fn status(&self) -> u16 {
        match self {
            Rejection::NotConfigured => 403,
            Rejection::BadCredentials => 401,
            Rejection::SecretsUnreadable => 500,
        }
    }

    pub(crate) fn message(&self, name: &str) -> String {
        match self {
            Rejection::NotConfigured => {
                format!("webhook '{name}' has no secret configured, so it admits nothing")
            }
            Rejection::BadCredentials => format!(
                "webhook '{name}' authentication failed: sign the body with HMAC-SHA256 \
                 (X-Hub-Signature-256: sha256=<hex>) or send the hook's own secret in \
                 X-Pher-Webhook-Token"
            ),
            Rejection::SecretsUnreadable => {
                "webhook secrets are unreadable; check webhooks.json".to_string()
            }
        }
    }
}

pub(crate) fn sanitize_name(raw: &str) -> String {
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

pub(crate) fn is_valid_name(name: &str) -> bool {
    !name.is_empty() && sanitize_name(name) == name
}

pub(crate) fn authenticate(
    paths: &Paths,
    name: &str,
    body: &[u8],
    header: impl Fn(&str) -> Option<String>,
) -> Result<(), Rejection> {
    let secret = secret_for(paths, name)?.ok_or(Rejection::NotConfigured)?;
    let signed = SIGNATURE_HEADERS
        .iter()
        .find_map(|h| header(h))
        .is_some_and(|sig| verify_hmac(&secret.value, body, &sig));
    let tokened = secret.hook_specific
        && TOKEN_HEADERS
            .iter()
            .find_map(|h| header(h))
            .is_some_and(|token| tokens_match(&secret.value, &token));
    if signed || tokened {
        Ok(())
    } else {
        Err(Rejection::BadCredentials)
    }
}

pub(crate) fn any_applied(paths: &Paths) -> bool {
    load(paths).is_ok_and(|defs| !defs.is_empty())
}

pub(crate) fn handle_rpc(request: Request, paths: &Paths) -> Value {
    match request {
        Request::WebhookSet { def } => set(paths, def),
        Request::WebhookLs => list(paths),
        Request::WebhookRm { name } => remove(paths, &name),
        _ => err("not a webhook op"),
    }
}

fn set(paths: &Paths, def: Value) -> Value {
    let Ok(def) = serde_json::from_value::<WebhookDef>(def) else {
        return err("invalid webhook: expected {name, secret} with string values");
    };
    if !is_valid_name(&def.name) {
        return err(format!(
            "webhook name '{}' must be lowercase letters, digits, '-' or '_'",
            sanitize_name(&def.name)
        ));
    }
    if def.secret.len() < MIN_SECRET_LEN {
        return err(format!(
            "webhook '{}': secret must be at least {MIN_SECRET_LEN} characters",
            def.name
        ));
    }
    let _guard = WRITE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut defs = match load(paths) {
        Ok(defs) => defs,
        Err(e) => return err(format!("cannot read webhooks.json: {e}")),
    };
    let name = def.name.clone();
    defs.retain(|d| d.name != name);
    defs.push(def);
    defs.sort_by(|a, b| a.name.cmp(&b.name));
    match save(paths, &defs) {
        Ok(()) => ok(json!({ "name": name })),
        Err(e) => err(format!("cannot persist webhook: {e}")),
    }
}

fn list(paths: &Paths) -> Value {
    match load(paths) {
        Ok(defs) => {
            let webhooks: Vec<Value> = defs
                .iter()
                .map(|d| {
                    json!({
                        "name": d.name,
                        "secretFingerprint": crate::connector::token_fingerprint(&d.secret),
                    })
                })
                .collect();
            ok(json!({ "webhooks": webhooks }))
        }
        Err(e) => err(format!("cannot read webhooks.json: {e}")),
    }
}

fn remove(paths: &Paths, name: &str) -> Value {
    let _guard = WRITE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut defs = match load(paths) {
        Ok(defs) => defs,
        Err(e) => return err(format!("cannot read webhooks.json: {e}")),
    };
    let before = defs.len();
    defs.retain(|d| d.name != name);
    let removed = defs.len() != before;
    if removed {
        if let Err(e) = save(paths, &defs) {
            return err(format!("cannot persist webhook removal: {e}"));
        }
    }
    ok(json!({ "removed": removed }))
}

pub(crate) fn warn_unusable_env_secrets() {
    for (var, value) in std::env::vars() {
        let is_secret_var = var == GLOBAL_ENV_VAR || var.starts_with("PHER_WEBHOOK_SECRET_");
        if is_secret_var && !value.is_empty() && value.len() < MIN_SECRET_LEN {
            eprintln!(
                "pherd: warning: {var} is shorter than {MIN_SECRET_LEN} characters; \
                 webhooks it covers reject every request"
            );
        }
    }
}

fn secret_for(paths: &Paths, name: &str) -> Result<Option<Secret>, Rejection> {
    let defs = load(paths).map_err(|_| Rejection::SecretsUnreadable)?;
    let secret = match defs
        .into_iter()
        .find(|d| d.name == name)
        .map(|d| d.secret)
        .or_else(|| env_secret(&env_var_for(name)))
    {
        Some(value) => Secret {
            value,
            hook_specific: true,
        },
        None => match env_secret(GLOBAL_ENV_VAR) {
            Some(value) => Secret {
                value,
                hook_specific: false,
            },
            None => return Ok(None),
        },
    };
    Ok((secret.value.len() >= MIN_SECRET_LEN).then_some(secret))
}

fn env_secret(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

fn env_var_for(name: &str) -> String {
    format!(
        "PHER_WEBHOOK_SECRET_{}",
        name.to_uppercase().replace('-', "_")
    )
}

fn load(paths: &Paths) -> Result<Vec<WebhookDef>, String> {
    match std::fs::read(paths.webhooks()) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|_| "webhooks.json is corrupt".into()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.kind().to_string()),
    }
}

fn save(paths: &Paths, defs: &[WebhookDef]) -> anyhow::Result<()> {
    let path = paths.webhooks();
    let tmp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(&serde_json::to_vec_pretty(defs)?)?;
    file.sync_all()?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

fn verify_hmac(secret: &str, body: &[u8], signature: &str) -> bool {
    let hex_part = signature.trim();
    let hex_part = hex_part.strip_prefix("sha256=").unwrap_or(hex_part);
    let Ok(expected) = hex::decode(hex_part) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
}

fn tokens_match(secret: &str, presented: &str) -> bool {
    let expected = Sha256::digest(secret.as_bytes());
    let actual = Sha256::digest(presented.as_bytes());
    expected.ct_eq(&actual).into()
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

        assert!(verify_hmac(secret, body, &format!("sha256={sig}")));
        assert!(verify_hmac(secret, body, &sig));
        assert!(!verify_hmac(secret, body, "sha256=deadbeef"));
        assert!(!verify_hmac(secret, body, ""));
        assert!(!verify_hmac("wrong", body, &format!("sha256={sig}")));
    }

    #[test]
    fn hook_names_sanitized() {
        assert_eq!(sanitize_name("Sentry"), "sentry");
        assert_eq!(sanitize_name("my_hook-1"), "my_hook-1");
        assert_eq!(sanitize_name("../etc/passwd"), "etc-passwd");
        assert_eq!(sanitize_name("//"), "");
    }

    #[test]
    fn hooks_without_a_secret_are_closed() {
        let home = std::env::temp_dir().join(format!("pher-webhook-test-{}", std::process::id()));
        std::fs::create_dir_all(&home).unwrap();
        let paths = Paths { home: home.clone() };
        let name = "closed-by-default-probe";
        let no_headers = |_: &str| None;
        assert!(matches!(
            authenticate(&paths, name, b"{}", no_headers),
            Err(Rejection::NotConfigured)
        ));

        let secret = "0123456789abcdef-probe";
        assert!(set(&paths, json!({"name": name, "secret": secret}))["ok"] == true);
        assert!(matches!(
            authenticate(&paths, name, b"{}", no_headers),
            Err(Rejection::BadCredentials)
        ));
        let token = |h: &str| (h == "x-pher-webhook-token").then(|| secret.to_string());
        assert!(authenticate(&paths, name, b"{}", token).is_ok());
        assert!(!list(&paths).to_string().contains(secret));

        let _ = std::fs::remove_dir_all(&home);
    }
}
