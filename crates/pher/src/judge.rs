//! Tier 4 (`judge`): a yes/no question posed to a cheap LLM about tier-3
//! survivors. Pluggable provider, one great default:
//!
//! - Anthropic `claude-haiku-4-5` (the default — haiku-class per spec), raw
//!   Messages API with prompt caching on the stable question prefix and
//!   structured outputs for a guaranteed `{verdict, rationale}` shape.
//! - OpenAI models (e.g. `gpt-5.6-luna`) via chat completions.
//!
//! Selection: `PHER_JUDGE_MODEL` (claude-* → Anthropic + ANTHROPIC_API_KEY,
//! gpt-*/o* → OpenAI + OPENAI_API_KEY). The envelope is redacted before it
//! leaves the process; budgets are enforced by the caller and fail closed.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const DEFAULT_MODEL: &str = "claude-haiku-4-5";
const MAX_EVENT_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Anthropic,
    OpenAI,
}

#[derive(Debug, Clone)]
pub struct JudgeConfig {
    pub provider: Provider,
    pub model: String,
    pub api_key: String,
    pub base_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub verdict: bool,
    pub rationale: String,
}

/// Resolve provider/model/key from the environment. Errors carry the exact
/// fix so registration failures are actionable.
pub fn resolve_config() -> Result<JudgeConfig, String> {
    let model = std::env::var("PHER_JUDGE_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string());
    let provider = if model.starts_with("claude") {
        Provider::Anthropic
    } else if model.starts_with("gpt") || model.starts_with('o') {
        Provider::OpenAI
    } else {
        match std::env::var("PHER_JUDGE_PROVIDER").as_deref() {
            Ok("anthropic") => Provider::Anthropic,
            Ok("openai") => Provider::OpenAI,
            _ => {
                return Err(format!(
                    "cannot infer provider for judge model '{model}'; set PHER_JUDGE_PROVIDER=anthropic|openai"
                ))
            }
        }
    };
    let (key_var, default_base) = match provider {
        Provider::Anthropic => ("ANTHROPIC_API_KEY", "https://api.anthropic.com"),
        Provider::OpenAI => ("OPENAI_API_KEY", "https://api.openai.com"),
    };
    let api_key = std::env::var(key_var).map_err(|_| {
        format!("judge model '{model}' needs {key_var} in the daemon's environment")
    })?;
    let base_url =
        std::env::var("PHER_JUDGE_BASE_URL").unwrap_or_else(|_| default_base.to_string());
    Ok(JudgeConfig {
        provider,
        model,
        api_key,
        base_url,
    })
}

/// The stable per-subscription prompt prefix. Kept byte-identical across
/// events so Anthropic prompt caching can engage on the question.
fn system_prompt(question: &str) -> String {
    format!(
        "You are the judge tier of the Pheromone event bus. You are shown one \
         event from an agent-infrastructure event stream and must answer one \
         yes/no question about it. Answer strictly as JSON: \
         {{\"verdict\": true|false, \"rationale\": \"<one short sentence>\"}}. \
         Judge only from the event's content; when genuinely uncertain, answer false.\n\n\
         Question: {question}"
    )
}

/// Redact and byte-cap the envelope before it leaves the process.
pub fn event_summary(event: &pher_core::Envelope) -> String {
    let text = serde_json::to_string(event).unwrap_or_default();
    let redacted = pher_embed::redact(&text);
    let mut out = redacted;
    if out.len() > MAX_EVENT_BYTES {
        let mut end = MAX_EVENT_BYTES;
        while !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
        out.push_str("…\"}");
    }
    out
}

/// Stable fingerprint of an event's CONTENT (type + subject + payload) —
/// deliberately excluding `id`/`ts`/`node`, so re-emitted identical content
/// hits the verdict cache instead of re-rolling.
pub fn content_fingerprint(event: &pher_core::Envelope) -> String {
    let content = serde_json::json!({
        "type": event.event_type,
        "subject": event.subject,
        "payload": event.payload,
    });
    fingerprint(&pher_embed::redact(&content.to_string()))
}

/// Cache key half: stable fingerprint of the (redacted) event content.
pub fn fingerprint(text: &str) -> String {
    // FNV-1a 64 — stable across runs, unlike the std RandomState hasher.
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in text.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

/// One blocking verdict call. Callers run this OFF the daemon state lock.
pub fn ask(cfg: &JudgeConfig, question: &str, event_summary: &str) -> Result<Verdict, String> {
    match cfg.provider {
        Provider::Anthropic => ask_anthropic(cfg, question, event_summary),
        Provider::OpenAI => ask_openai(cfg, question, event_summary),
    }
}

fn ask_anthropic(
    cfg: &JudgeConfig,
    question: &str,
    event_summary: &str,
) -> Result<Verdict, String> {
    let body = json!({
        "model": cfg.model,
        "max_tokens": 256,
        "system": [{
            "type": "text",
            "text": system_prompt(question),
            // Stable prefix; harmless below the model's cacheable minimum.
            "cache_control": {"type": "ephemeral"},
        }],
        "messages": [{"role": "user", "content": event_summary}],
        "output_config": {
            "format": {
                "type": "json_schema",
                "schema": {
                    "type": "object",
                    "properties": {
                        "verdict": {"type": "boolean"},
                        "rationale": {"type": "string"},
                    },
                    "required": ["verdict", "rationale"],
                    "additionalProperties": false,
                },
            },
        },
    });
    let response = ureq::post(&format!("{}/v1/messages", cfg.base_url))
        .set("x-api-key", &cfg.api_key)
        .set("anthropic-version", "2023-06-01")
        .set("content-type", "application/json")
        .timeout(std::time::Duration::from_secs(30))
        .send_string(&body.to_string())
        .map_err(|e| format!("anthropic request failed: {e}"))?;
    let text_body = response
        .into_string()
        .map_err(|e| format!("anthropic response unreadable: {e}"))?;
    let value: Value = serde_json::from_str(&text_body)
        .map_err(|e| format!("anthropic response not JSON: {e}"))?;
    if value.get("stop_reason").and_then(|s| s.as_str()) == Some("refusal") {
        // Safety classifiers declined — fail closed, no verdict.
        return Err("judge model refused the request".to_string());
    }
    let text = value
        .get("content")
        .and_then(|c| c.as_array())
        .and_then(|blocks| {
            blocks
                .iter()
                .find(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
        })
        .and_then(|b| b.get("text"))
        .and_then(|t| t.as_str())
        .ok_or_else(|| format!("anthropic response had no text block: {value}"))?;
    parse_verdict(text)
}

fn ask_openai(cfg: &JudgeConfig, question: &str, event_summary: &str) -> Result<Verdict, String> {
    let body = json!({
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": system_prompt(question)},
            {"role": "user", "content": event_summary},
        ],
    });
    let response = ureq::post(&format!("{}/v1/chat/completions", cfg.base_url))
        .set("authorization", &format!("Bearer {}", cfg.api_key))
        .set("content-type", "application/json")
        .timeout(std::time::Duration::from_secs(30))
        .send_string(&body.to_string())
        .map_err(|e| format!("openai request failed: {e}"))?;
    let text_body = response
        .into_string()
        .map_err(|e| format!("openai response unreadable: {e}"))?;
    let value: Value =
        serde_json::from_str(&text_body).map_err(|e| format!("openai response not JSON: {e}"))?;
    let text = value
        .pointer("/choices/0/message/content")
        .and_then(|t| t.as_str())
        .ok_or_else(|| format!("openai response had no message content: {value}"))?;
    parse_verdict(text)
}

/// Parse `{verdict, rationale}` from model text, tolerating prose or code
/// fences around the JSON object.
fn parse_verdict(text: &str) -> Result<Verdict, String> {
    let candidate = match text.find('{') {
        Some(start) => match text.rfind('}') {
            Some(end) if end > start => &text[start..=end],
            _ => text,
        },
        None => text,
    };
    if let Ok(v) = serde_json::from_str::<Value>(candidate) {
        if let Some(verdict) = v.get("verdict").and_then(|b| b.as_bool()) {
            let rationale = v
                .get("rationale")
                .and_then(|r| r.as_str())
                .unwrap_or("")
                .to_string();
            return Ok(Verdict { verdict, rationale });
        }
    }
    // Last resort: a bare yes/no style answer.
    let lower = text.to_lowercase();
    if lower.contains("\"verdict\": true") || lower.trim_start().starts_with("yes") {
        return Ok(Verdict {
            verdict: true,
            rationale: text.chars().take(200).collect(),
        });
    }
    if lower.contains("\"verdict\": false") || lower.trim_start().starts_with("no") {
        return Ok(Verdict {
            verdict: false,
            rationale: text.chars().take(200).collect(),
        });
    }
    Err(format!("could not parse judge verdict from: {text}"))
}

/// Budget-period length in seconds.
pub fn period_secs(period: &str) -> u64 {
    match period {
        "min" => 60,
        "hour" => 3600,
        "day" => 86400,
        "week" => 604800,
        _ => 86400,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_clean_and_messy_verdicts() {
        let v = parse_verdict(r#"{"verdict": true, "rationale": "stuck in a loop"}"#).unwrap();
        assert!(v.verdict);
        assert_eq!(v.rationale, "stuck in a loop");

        let v = parse_verdict("```json\n{\"verdict\": false, \"rationale\": \"healthy\"}\n```")
            .unwrap();
        assert!(!v.verdict);

        let v = parse_verdict("Yes, this agent is clearly stuck.").unwrap();
        assert!(v.verdict);

        assert!(parse_verdict("maybe, hard to say").is_err());
    }

    #[test]
    fn content_fingerprint_ignores_id_and_ts() {
        let mut a = pher_core::Envelope::new("hive.seal", serde_json::json!({"x": 1}));
        a.id = "PH.aaa".into();
        a.ts = "2026-08-09T10:00:00Z".into();
        let mut b = pher_core::Envelope::new("hive.seal", serde_json::json!({"x": 1}));
        b.id = "PH.bbb".into();
        b.ts = "2026-08-09T11:00:00Z".into();
        assert_eq!(content_fingerprint(&a), content_fingerprint(&b));
        let c = pher_core::Envelope::new("hive.seal", serde_json::json!({"x": 2}));
        assert_ne!(content_fingerprint(&a), content_fingerprint(&c));
    }

    #[test]
    fn fingerprint_is_stable_and_distinct() {
        assert_eq!(fingerprint("abc"), fingerprint("abc"));
        assert_ne!(fingerprint("abc"), fingerprint("abd"));
        assert_eq!(fingerprint("abc").len(), 16);
    }

    #[test]
    fn event_summary_redacts_and_caps() {
        let mut e = pher_core::Envelope::new(
            "hive.seal",
            serde_json::json!({"token": "sk-ant-abc123def456ghi789", "log": "x".repeat(10_000)}),
        );
        e.id = "PH.x".into();
        let s = event_summary(&e);
        assert!(!s.contains("sk-ant-abc123def456"), "secret leaked to judge");
        assert!(s.len() <= MAX_EVENT_BYTES + 8);
    }

    #[test]
    fn provider_inference() {
        // Can't set env vars safely in parallel tests; test the prompt shape.
        let p = system_prompt("Is this agent stuck?");
        assert!(p.contains("Question: Is this agent stuck?"));
        assert!(p.contains("verdict"));
    }

    #[test]
    fn periods() {
        assert_eq!(period_secs("min"), 60);
        assert_eq!(period_secs("day"), 86400);
        assert_eq!(period_secs("week"), 604800);
    }
}
