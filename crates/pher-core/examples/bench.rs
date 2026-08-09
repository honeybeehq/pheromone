//! Tier 1–2 throughput harness: N standing subscriptions × M events.
//! Roadmap target: 50k events/sec with 10k subscriptions on developer hardware.
//!
//! Run with: cargo run --release -p pher-core --example bench

use std::time::Instant;

use pher_core::{Envelope, Matcher, Subscription};
use serde_json::json;

const NUM_SUBS: usize = 10_000;
const NUM_EVENTS: usize = 100_000;

fn main() {
    let namespaces = [
        "hive", "pol", "ci", "metric", "crash", "vercel", "posthog", "apiary",
    ];
    let kinds = [
        "seal",
        "spawned",
        "failed",
        "completed",
        "entered",
        "cleared",
        "oom",
        "lagging",
    ];

    let mut matcher = Matcher::new();
    for i in 0..NUM_SUBS {
        let ns = namespaces[i % namespaces.len()];
        let kind = kinds[(i / namespaces.len()) % kinds.len()];
        // Mix of exact, star, and double-star subscriptions with selective
        // where clauses (realistic fleets: most candidates fail tier 2).
        let sub_str = match i % 3 {
            0 => format!(
                "on {ns}.{kind} where payload.n == {} then cmd true",
                i % 100
            ),
            1 => format!(
                "on {ns}.*.{kind} where payload.env == \"prod\" && payload.n == {} then cmd true",
                i % 100
            ),
            _ => format!(
                "on {ns}.** where has(payload.n) && payload.n == {} then cmd true",
                i % 100
            ),
        };
        let sub = Subscription::parse(&sub_str).expect("bench sub parses");
        matcher.insert(format!("S{i}"), sub);
    }

    let events: Vec<Envelope> = (0..NUM_EVENTS)
        .map(|i| {
            let ns = namespaces[i % namespaces.len()];
            let kind = kinds[(i / 7) % kinds.len()];
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
        .collect();

    let start = Instant::now();
    let mut matched = 0u64;
    for event in &events {
        matched += matcher.match_ids(event).len() as u64;
    }
    let elapsed = start.elapsed();
    let eps = NUM_EVENTS as f64 / elapsed.as_secs_f64();

    println!("subscriptions:      {NUM_SUBS}");
    println!("events:             {NUM_EVENTS}");
    println!("elapsed:            {elapsed:?}");
    println!("events/sec:         {eps:.0}");
    println!("matches:            {matched}");
    println!(
        "matches/event:      {:.1}",
        matched as f64 / NUM_EVENTS as f64
    );
}
