//! Tier 1–2 throughput harness: N standing subscriptions × M events.
//! Roadmap target: 50k events/sec with 10k subscriptions on developer hardware.
//!
//! Two scenarios:
//! - "worst case": a third of all subscriptions are `ns.**` catch-alls, so
//!   every event has ~470 trie candidates. Candidate count is the cost driver.
//! - "realistic": 2% catch-alls, exact/star otherwise — the shape a real
//!   fleet has, tens of candidates per event.
//!
//! Run with: cargo run --release -p pher-core --example bench

use std::time::Instant;

use pher_core::matcher::Scratch;
use pher_core::{Envelope, Matcher, Subscription};
use serde_json::json;

const NUM_SUBS: usize = 10_000;
const NUM_EVENTS: usize = 100_000;

const NAMESPACES: [&str; 8] = [
    "hive", "pol", "ci", "metric", "crash", "vercel", "posthog", "apiary",
];
const KINDS: [&str; 8] = [
    "seal",
    "spawned",
    "failed",
    "completed",
    "entered",
    "cleared",
    "oom",
    "lagging",
];

fn build_matcher(dstar_one_in: usize) -> Matcher {
    let mut matcher = Matcher::new();
    for i in 0..NUM_SUBS {
        let ns = NAMESPACES[i % NAMESPACES.len()];
        let kind = KINDS[(i / NAMESPACES.len()) % KINDS.len()];
        let sub_str = if i % dstar_one_in == 0 {
            format!(
                "on {ns}.** where has(payload.n) && payload.n == {} then cmd true",
                i % 100
            )
        } else if i % 3 == 1 {
            format!(
                "on {ns}.*.{kind} where payload.env == \"prod\" && payload.n == {} then cmd true",
                i % 100
            )
        } else {
            format!(
                "on {ns}.{kind} where payload.n == {} then cmd true",
                i % 100
            )
        };
        let sub = Subscription::parse(&sub_str).expect("bench sub parses");
        matcher.insert(format!("S{i}"), sub);
    }
    matcher
}

fn build_events() -> Vec<Envelope> {
    (0..NUM_EVENTS)
        .map(|i| {
            let ns = NAMESPACES[i % NAMESPACES.len()];
            let kind = KINDS[(i / 7) % KINDS.len()];
            let mut e = Envelope::new(
                format!("{ns}.{}.{kind}", i % 5),
                json!({"n": i % 100, "env": if i % 4 == 0 { "prod" } else { "dev" }}),
            );
            e.id = format!("PH.b{i}");
            e.ts = "2026-08-09T12:00:00Z".to_string();
            e.node = "bench".to_string();
            e.source = "bench".to_string();
            e
        })
        .collect()
}

fn run(name: &str, matcher: &Matcher, events: &[Envelope]) {
    let start = Instant::now();
    let mut matched = 0u64;
    let mut scratch = Scratch::new();
    let mut hits: Vec<&str> = Vec::new();
    for event in events {
        matcher.match_ids_with(event, &mut scratch, &mut hits);
        matched += hits.len() as u64;
    }
    let elapsed = start.elapsed();
    let eps = NUM_EVENTS as f64 / elapsed.as_secs_f64();
    println!("[{name}]");
    println!("  elapsed:        {elapsed:?}");
    println!("  events/sec:     {eps:.0}");
    println!(
        "  matches/event:  {:.1}",
        matched as f64 / NUM_EVENTS as f64
    );
}

fn main() {
    let events = build_events();
    println!("{NUM_SUBS} subscriptions x {NUM_EVENTS} events, tiers 1-2\n");
    run(
        "worst case: 33% ns.** catch-alls",
        &build_matcher(3),
        &events,
    );
    run(
        "realistic:   2% ns.** catch-alls",
        &build_matcher(50),
        &events,
    );
}
