use pher_core::matcher::{evaluate, why_not, Outcome};
use pher_core::subscription::Lifetime;
use pher_core::{Envelope, Subscription};
use serde_json::json;

const LEASED: &str = r#"when on hive.flag.set where payload.flag.flag == "spawn_failed" then stream while apiary-41c0 alive"#;
const UNLEASED: &str =
    r#"when on hive.flag.set where payload.flag.flag == "spawn_failed" then stream"#;

fn spawn_failed() -> Envelope {
    Envelope::new(
        "hive.flag.set",
        json!({ "flag": { "flag": "spawn_failed" } }),
    )
}

#[test]
fn stream_rule_with_lease_parses_into_lease_and_round_trips() {
    let sub = Subscription::parse(LEASED).expect("leased stream rule parses");
    assert_eq!(
        sub.lifetime,
        Lifetime::Lease {
            lessee: "apiary-41c0".into()
        }
    );
    assert_eq!(sub.canon(), LEASED);
    assert_eq!(Subscription::parse(&sub.canon()).unwrap(), sub);
}

#[test]
fn lease_never_affects_matching() {
    let leased = Subscription::parse(LEASED).unwrap();
    let unleased = Subscription::parse(UNLEASED).unwrap();
    let event = spawn_failed();

    let with_lease = evaluate("SUB", &leased, &event, None);
    let without_lease = evaluate("SUB", &unleased, &event, None);
    assert!(matches!(with_lease.outcome, Outcome::Matched));
    assert_eq!(
        serde_json::to_value(&with_lease).unwrap(),
        serde_json::to_value(&without_lease).unwrap()
    );

    let why_leased = why_not("SUB", &leased, &event);
    assert_eq!(why_leased["matched"], true);
    assert_eq!(why_leased, why_not("SUB", &unleased, &event));
}

#[test]
fn ttl_and_lease_stay_mutually_exclusive() {
    let err = Subscription::parse("when on a then stream for 7d while x alive")
        .expect_err("for + while must be rejected");
    assert!(
        err.to_string()
            .contains("'for' and 'while' are mutually exclusive"),
        "unexpected error: {err}"
    );
}
