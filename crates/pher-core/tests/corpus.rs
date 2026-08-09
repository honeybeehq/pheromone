//! Golden corpus: every worked example from docs/LANGUAGE.md must parse,
//! round-trip through the canonical string and JSON forms, and match/reject
//! realistic events the way the spec says.

use pher_core::matcher::{evaluate, Outcome};
use pher_core::{Envelope, Subscription};
use serde_json::json;

/// The worked examples from LANGUAGE.md (options inline, as string form).
const WORKED_EXAMPLES: &[&str] = &[
    r#"on hive.seal where payload.status == "blocked" then buz CL.6308 --tier next-tool while CL.6308 alive"#,
    r#"on ci.github.run.completed where payload.conclusion == "failure" && payload.branch == "main" meaning "infra flake or transient runner failure, not a code bug" > 0.8 then hive spawn --flow rerun-and-triage"#,
    r#"on hive.** meaning novel over 7d then buz operator every 30m"#,
    r#"on hive.session.* judge "Is this agent stuck in a loop, repeating the same failing action?" budget 200/day then buz operator --tier queue"#,
    r#"on hive.bee.spawned expect hive.seal where correlation == $origin.correlation within 2h else then buz operator"#,
    r#"on crash.sentry.* meaning "OOM or memory pressure" then cmd ./triage.sh since 24h"#,
    r#"on metric.condition.entered where payload.name == "p95_latency" && payload.env == "prod" then hermes notify-oncall"#,
    r#"when on deploy.finished where payload.env == "prod" then emit deploy.prod.finished limit 1 for 30m"#,
    r#"on hive.seal, pol.job.failed from trmd-mbp then cmd echo multi-subject"#,
    r#"on ** where size(payload) > 0 then cmd echo catch-all batch 1h"#,
];

#[test]
fn worked_examples_parse_and_round_trip() {
    for src in WORKED_EXAMPLES {
        let sub = Subscription::parse(src)
            .unwrap_or_else(|e| panic!("failed to parse example:\n  {src}\n  error: {e}"));

        // String round-trip: canon is idempotent and parses to the same value.
        let canon = sub.canon();
        let reparsed = Subscription::parse(&canon)
            .unwrap_or_else(|e| panic!("canonical form failed to re-parse:\n  {canon}\n  {e}"));
        assert_eq!(
            sub, reparsed,
            "string round-trip changed:\n  {src}\n  {canon}"
        );
        assert_eq!(canon, reparsed.canon(), "canon not idempotent for {src}");

        // JSON round-trip.
        let js = sub.to_json();
        let from_js = Subscription::from_json(&js)
            .unwrap_or_else(|e| panic!("JSON round-trip failed for {src}: {e}"));
        assert_eq!(sub, from_js, "JSON round-trip changed:\n  {src}");
    }
}

#[test]
fn rejects_from_the_spec() {
    let bad = [
        // on is mandatory.
        r#"where payload.x == 1 then cmd echo hi"#,
        // ** must be terminal.
        r#"on hive.**.seal then cmd echo hi"#,
        // judge budget is mandatory.
        r#"on hive.seal judge "stuck?" then cmd echo hi"#,
        // unknown sink.
        r#"on hive.seal then teleport somewhere"#,
        // unknown root identifier in where.
        r#"on hive.seal where data.x == 1 then cmd echo hi"#,
        // clause out of cascade order.
        r#"on hive.seal meaning "x" where payload.a == 1 then cmd echo hi"#,
        // missing then.
        r#"on hive.seal"#,
        // invalid regex.
        r#"on a where payload.x matches "[" then cmd echo hi"#,
        // threshold out of range.
        r#"on a meaning "x" > 1.5 then cmd echo hi"#,
        // limit 0.
        r#"on a then cmd echo hi limit 0"#,
    ];
    for src in bad {
        assert!(
            Subscription::parse(src).is_err(),
            "expected parse error for:\n  {src}"
        );
    }
}

fn ev(
    subject: &str,
    source: &str,
    correlation: Option<&str>,
    payload: serde_json::Value,
) -> Envelope {
    Envelope {
        id: "PH.gold".into(),
        ts: "2026-08-09T12:00:00Z".into(),
        node: "trmd-mbp".into(),
        source: source.into(),
        event_type: subject.into(),
        subject: subject.into(),
        correlation: correlation.map(|s| s.to_string()),
        payload,
        ttl_class: None,
        hops: None,
    }
}

/// Events shaped like the seed vocabulary (hive ledger, CI, metrics, crashes).
#[test]
fn golden_matching_cases() {
    let blocked_seal = ev(
        "hive.seal",
        "tap.hive",
        Some("HE.a3f"),
        json!({"status": "blocked", "bee": "CL.6308", "reason": "awaiting credentials"}),
    );
    let done_seal = ev(
        "hive.seal",
        "tap.hive",
        Some("HE.a3f"),
        json!({"status": "done", "bee": "CL.6308"}),
    );
    let ci_fail_main = ev(
        "ci.github.run.completed",
        "tap.github",
        None,
        json!({"conclusion": "failure", "branch": "main", "run_id": 812, "repo": "honeybee/pheromone"}),
    );
    let ci_fail_branch = ev(
        "ci.github.run.completed",
        "tap.github",
        None,
        json!({"conclusion": "failure", "branch": "feat/x", "run_id": 813}),
    );
    let metric = ev(
        "metric.condition.entered",
        "tap.metrics",
        None,
        json!({"name": "p95_latency", "env": "prod", "value_ms": 950, "threshold_ms": 800}),
    );

    // (subscription, event, expected outcome-kind, rejecting tier if any)
    let cases: Vec<(&str, &Envelope, &str, Option<&str>)> = vec![
        (
            r#"on hive.seal where payload.status == "blocked" then cmd echo hit"#,
            &blocked_seal,
            "matched",
            None,
        ),
        (
            r#"on hive.seal where payload.status == "blocked" then cmd echo hit"#,
            &done_seal,
            "rejected",
            Some("where"),
        ),
        (
            r#"on hive.** where has(payload.bee) then cmd echo hit"#,
            &blocked_seal,
            "matched",
            None,
        ),
        (
            r#"on pol.job.* then cmd echo hit"#,
            &blocked_seal,
            "rejected",
            Some("on"),
        ),
        (
            r#"on ci.github.run.completed where payload.conclusion == "failure" && payload.branch == "main" then cmd echo hit"#,
            &ci_fail_main,
            "matched",
            None,
        ),
        (
            r#"on ci.github.run.completed where payload.conclusion == "failure" && payload.branch == "main" then cmd echo hit"#,
            &ci_fail_branch,
            "rejected",
            Some("where"),
        ),
        (
            r#"on ci.github.run.completed from tap.github where payload.run_id > 800 then cmd echo hit"#,
            &ci_fail_main,
            "matched",
            None,
        ),
        (
            r#"on ci.github.run.completed from tap.gitlab then cmd echo hit"#,
            &ci_fail_main,
            "rejected",
            Some("from"),
        ),
        (
            r#"on metric.condition.entered where payload.name == "p95_latency" && payload.env == "prod" then hermes notify-oncall"#,
            &metric,
            "matched",
            None,
        ),
        (
            r#"on metric.condition.entered where payload.name in ["error_rate", "p95_latency"] then cmd echo hit"#,
            &metric,
            "matched",
            None,
        ),
        (
            r#"on hive.seal where payload.reason matches "credential|auth" then cmd echo hit"#,
            &blocked_seal,
            "matched",
            None,
        ),
        (
            // meaning present → never a silent match in this build
            r#"on hive.seal meaning "stuck on auth" then cmd echo hit"#,
            &blocked_seal,
            "pending",
            None,
        ),
    ];

    for (src, event, expected, tier) in cases {
        let sub = Subscription::parse(src).unwrap_or_else(|e| panic!("parse {src}: {e}"));
        let eval = evaluate("S", &sub, event, None);
        match (&eval.outcome, expected) {
            (Outcome::Matched, "matched") => {}
            (Outcome::PendingSemantic { .. }, "pending") => {}
            (Outcome::Rejected { tier: t, .. }, "rejected") => {
                assert_eq!(Some(t.as_str()), tier, "wrong rejecting tier for {src}");
            }
            (got, want) => panic!(
                "wrong outcome for\n  sub: {src}\n  event: {}\n  want {want}, got {got:?}",
                event.subject
            ),
        }
    }
}

#[test]
fn match_block_shape_follows_delivery_contract() {
    let sub = Subscription::parse(
        r#"on ci.github.run.completed where payload.conclusion == "failure" then cmd echo hit"#,
    )
    .unwrap();
    let e = ev(
        "ci.github.run.completed",
        "tap.github",
        Some("HE.a3f"),
        json!({"conclusion": "failure"}),
    );
    let eval = evaluate("PH.4k2", &sub, &e, None);
    let block = serde_json::to_value(eval.match_block.unwrap()).unwrap();
    assert_eq!(block["subscription"], "PH.4k2");
    assert_eq!(block["tiers"], json!(["on", "where"]));
    assert_eq!(block["where"]["result"], json!(true));
    assert_eq!(
        block["where"]["expr"],
        json!(r#"payload.conclusion == "failure""#)
    );
    assert_eq!(block["meaning"], json!(null));
    assert_eq!(block["judge"], json!(null));
}

#[test]
fn canonical_json_matches_spec_example() {
    // The canonical JSON example from LANGUAGE.md.
    let sub = Subscription::parse(
        r#"on ci.github.run.completed where payload.conclusion == "failure" && payload.branch == "main" meaning "infra flake or transient runner failure, not a code bug" > 0.8 then hive spawn --flow rerun-and-triage"#,
    )
    .unwrap();
    let js = sub.to_json();
    assert_eq!(js["on"], json!(["ci.github.run.completed"]));
    assert_eq!(
        js["where"],
        json!(r#"payload.conclusion == "failure" && payload.branch == "main""#)
    );
    assert_eq!(
        js["meaning"],
        json!({"descriptors": ["infra flake or transient runner failure, not a code bug"], "threshold": 0.8})
    );
    assert_eq!(js["judge"], json!(null));
    assert_eq!(
        js["then"],
        json!({"sink": "hive", "args": ["spawn", "--flow", "rerun-and-triage"]})
    );
    assert_eq!(js["lifetime"], json!({"kind": "durable"}));
    assert_eq!(js["delivery"], json!({"mode": "immediate"}));
    assert_eq!(js["replay"], json!(null));
    assert_eq!(js["limit"], json!(null));
}

#[test]
fn expect_join_binds_origin() {
    let sub = Subscription::parse(
        r#"on hive.bee.spawned expect hive.seal where correlation == $origin.correlation within 2h else then buz operator"#,
    )
    .unwrap();
    let expect = sub.expect.as_ref().unwrap();
    assert_eq!(expect.subject.to_string(), "hive.seal");
    assert_eq!(expect.within.secs(), 7200);

    let origin = ev("hive.bee.spawned", "tap.hive", Some("HE.42"), json!({}));
    let sealed_same = ev("hive.seal", "tap.hive", Some("HE.42"), json!({}));
    let sealed_other = ev("hive.seal", "tap.hive", Some("HE.99"), json!({}));

    let ctx_ok = pher_core::EvalCtx {
        event: &sealed_same,
        origin: Some(&origin),
    };
    let ctx_no = pher_core::EvalCtx {
        event: &sealed_other,
        origin: Some(&origin),
    };
    let w = expect.where_expr.as_ref().unwrap();
    assert!(pher_core::expr::eval_bool(w, &ctx_ok).unwrap());
    assert!(!pher_core::expr::eval_bool(w, &ctx_no).unwrap());
}

#[test]
fn options_parse_and_serialize() {
    let sub = Subscription::parse(
        r#"on hive.seal then cmd echo hi while CL.6308 alive every 30m since 24h reevaluate limit 5"#,
    )
    .unwrap();
    assert_eq!(
        sub.lifetime,
        pher_core::Lifetime::Lease {
            lessee: "CL.6308".into()
        }
    );
    assert!(
        matches!(sub.delivery, pher_core::Delivery::Debounce { ref window } if window.text() == "30m")
    );
    let replay = sub.replay.as_ref().unwrap();
    assert_eq!(replay.lookback.text(), "24h");
    assert!(replay.reevaluate);
    assert_eq!(sub.limit, Some(5));

    // Options that collide with action args: the greedy suffix rule eats them.
    let sub = Subscription::parse(r#"on a then cmd echo limit 3"#).unwrap();
    assert_eq!(sub.limit, Some(3));
    assert_eq!(sub.then.args, vec!["echo"]);
    // Quoting protects the arg.
    let sub = Subscription::parse(r#"on a then cmd echo "limit 3""#).unwrap();
    assert_eq!(sub.limit, None);
    assert_eq!(sub.then.args, vec!["echo", "limit 3"]);
}
