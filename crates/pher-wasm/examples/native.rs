use pher_core::matcher::{self, Check};
use pher_core::{Envelope, Subscription};
use serde_json::{json, Value};

fn main() {
    let mut args = std::env::args().skip(1);
    let (Some(rule), Some(event_json)) = (args.next(), args.next()) else {
        eprintln!("usage: native <rule> <envelope-json>");
        std::process::exit(2);
    };
    let sub = Subscription::parse(&rule).unwrap_or_else(|e| panic!("rule does not parse: {e}"));
    let envelope: Envelope = serde_json::from_str(&event_json).expect("envelope JSON");
    let validate = Subscription::parse(&rule).err().map(|e| e.to_string());
    let evaluate = serde_json::to_value(matcher::evaluate("SUB", &sub, &envelope, None)).unwrap();
    let survives = matches!(
        matcher::check(&sub, &envelope, None),
        Check::Matched | Check::Pending
    );
    let output: Value = json!({
        "parse": sub.to_json(),
        "canon": sub.canon(),
        "validate": validate,
        "evaluate": evaluate,
        "survives": survives,
        "whyNot": matcher::why_not("SUB", &sub, &envelope),
    });
    println!("{output}");
}
