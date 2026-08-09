// The tier-3 quality harness (gates shipping tier 3 per ROADMAP slice 4):
// labeled (descriptor, event, should-match) triples across coarse topical
// routing and fine-grained distinctions; reports separation and the
// accuracy of candidate thresholds.
//
// Run: cargo run --release -p pher-embed --example quality

use pher_core::Envelope;
use serde_json::json;

struct Case {
    descriptor: &'static str,
    event: Envelope,
    should_match: bool,
    kind: &'static str, // "coarse" | "fine"
}

fn ev(subject: &str, payload: serde_json::Value) -> Envelope {
    Envelope::new(subject, payload)
}

fn main() -> anyhow::Result<()> {
    let embedder = pher_embed::Embedder::new("/tmp/pher-models".into())?;

    let cases = vec![
        // -- coarse topical routing ------------------------------------------
        Case {
            descriptor: "out of memory or memory pressure crash",
            event: ev(
                "crash.sentry.backend",
                json!({"title": "OOMKilled: worker exceeded memory limit 2Gi", "level": "fatal"}),
            ),
            should_match: true,
            kind: "coarse",
        },
        Case {
            descriptor: "out of memory or memory pressure crash",
            event: ev(
                "crash.sentry.backend",
                json!({"title": "TypeError: cannot read property 'id' of undefined", "level": "error"}),
            ),
            should_match: false,
            kind: "coarse",
        },
        Case {
            descriptor: "agent stuck waiting on authentication or credentials",
            event: ev(
                "hive.seal",
                json!({"status": "blocked", "reason": "awaiting OAuth token refresh, credential expired"}),
            ),
            should_match: true,
            kind: "coarse",
        },
        Case {
            descriptor: "agent stuck waiting on authentication or credentials",
            event: ev(
                "hive.seal",
                json!({"status": "done", "reason": "all tests green, merged PR"}),
            ),
            should_match: false,
            kind: "coarse",
        },
        Case {
            descriptor: "database connection problems or connection pool exhaustion",
            event: ev(
                "metric.condition.entered",
                json!({"name": "pg_pool_wait", "detail": "postgres connection pool saturated, clients queuing"}),
            ),
            should_match: true,
            kind: "coarse",
        },
        Case {
            descriptor: "database connection problems or connection pool exhaustion",
            event: ev(
                "metric.condition.entered",
                json!({"name": "p95_latency", "detail": "render latency above 800ms on marketing pages"}),
            ),
            should_match: false,
            kind: "coarse",
        },
        Case {
            descriptor: "deployment or release finished successfully",
            event: ev(
                "vercel.deploy.succeeded",
                json!({"project": "web", "state": "READY", "detail": "production deployment complete"}),
            ),
            should_match: true,
            kind: "coarse",
        },
        Case {
            descriptor: "deployment or release finished successfully",
            event: ev(
                "posthog.event.captured",
                json!({"event": "pageview", "path": "/pricing"}),
            ),
            should_match: false,
            kind: "coarse",
        },
        // -- fine distinctions (documented hard: this is judge-tier territory)
        Case {
            descriptor: "infra flake or transient runner failure, not a code bug",
            event: ev(
                "ci.github.run.completed",
                json!({"conclusion": "failure", "failure_reason": "runner lost communication with the server, network timeout during setup"}),
            ),
            should_match: true,
            kind: "fine",
        },
        Case {
            descriptor: "infra flake or transient runner failure, not a code bug",
            event: ev(
                "ci.github.run.completed",
                json!({"conclusion": "failure", "failure_reason": "assertion failed: expected 4 but got 5 in parser::tests::round_trip"}),
            ),
            should_match: false,
            kind: "fine",
        },
        Case {
            descriptor: "user-facing outage, customers affected",
            event: ev(
                "crash.sentry.frontend",
                json!({"title": "checkout page renders blank for all users on submit", "level": "fatal"}),
            ),
            should_match: true,
            kind: "fine",
        },
        Case {
            descriptor: "user-facing outage, customers affected",
            event: ev(
                "crash.sentry.backend",
                json!({"title": "background job retry succeeded after transient S3 error", "level": "warning"}),
            ),
            should_match: false,
            kind: "fine",
        },
    ];

    // Embed all descriptors and projections in one batch.
    let mut texts: Vec<String> = Vec::new();
    for c in &cases {
        texts.push(c.descriptor.to_string());
        texts.push(pher_embed::project(&c.event));
    }
    let vecs = embedder.embed(texts)?;

    let scores: Vec<f32> = cases
        .iter()
        .enumerate()
        .map(|(i, _)| pher_embed::cosine(&vecs[2 * i], &vecs[2 * i + 1]))
        .collect();

    println!(
        "{:<8} {:<7} {:>6}  descriptor | event",
        "kind", "should", "score"
    );
    for (c, s) in cases.iter().zip(&scores) {
        println!(
            "{:<8} {:<7} {:>6.3}  {} | {}",
            c.kind,
            c.should_match,
            s,
            &c.descriptor[..c.descriptor.len().min(40)],
            c.event.subject
        );
    }

    for threshold in [0.55f32, 0.60, 0.65, 0.70, 0.75] {
        let mut tp = 0;
        let mut fp = 0;
        let mut tn = 0;
        let mut fneg = 0;
        for (c, s) in cases.iter().zip(&scores) {
            match (c.should_match, *s > threshold) {
                (true, true) => tp += 1,
                (false, true) => fp += 1,
                (false, false) => tn += 1,
                (true, false) => fneg += 1,
            }
        }
        println!(
            "threshold {threshold:.2}: accuracy {}/{} (tp {tp} fp {fp} tn {tn} fn {fneg})",
            tp + tn,
            cases.len()
        );
    }
    Ok(())
}
