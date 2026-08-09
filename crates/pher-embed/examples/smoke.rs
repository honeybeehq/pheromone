// Projection spike (ROADMAP open question 1): compare candidate projections
// and descriptor phrasings for discrimination on labeled pairs.
use pher_core::Envelope;
use serde_json::json;

fn main() -> anyhow::Result<()> {
    let e = pher_embed::Embedder::new("/tmp/pher-models".into())?;
    let descriptor = "infra flake or transient runner failure, not a code bug";

    let flake = Envelope::new(
        "ci.github.run.completed",
        json!({
            "conclusion": "failure", "branch": "main",
            "failure_reason": "runner lost communication with the server, network timeout during setup"
        }),
    );
    let realbug = Envelope::new(
        "ci.github.run.completed",
        json!({
            "conclusion": "failure", "branch": "main",
            "failure_reason": "assertion failed: expected 4 but got 5 in parser::tests::round_trip"
        }),
    );

    let variants: Vec<(&str, String, String, String)> = vec![
        (
            "plain / full projection",
            descriptor.to_string(),
            pher_embed::project(&flake),
            pher_embed::project(&realbug),
        ),
        (
            "bge query prefix / full projection",
            format!("Represent this sentence for searching relevant passages: {descriptor}"),
            pher_embed::project(&flake),
            pher_embed::project(&realbug),
        ),
        (
            "plain / payload-only",
            descriptor.to_string(),
            "runner lost communication with the server, network timeout during setup".into(),
            "assertion failed: expected 4 but got 5 in parser::tests::round_trip".into(),
        ),
        (
            "bge query prefix / payload-only",
            format!("Represent this sentence for searching relevant passages: {descriptor}"),
            "runner lost communication with the server, network timeout during setup".into(),
            "assertion failed: expected 4 but got 5 in parser::tests::round_trip".into(),
        ),
    ];

    for (name, d, f, r) in variants {
        let vecs = e.embed(vec![d, f, r])?;
        let sf = pher_embed::cosine(&vecs[0], &vecs[1]);
        let sr = pher_embed::cosine(&vecs[0], &vecs[2]);
        println!(
            "{name:40} flake={sf:.3} realbug={sr:.3} delta={:+.3}",
            sf - sr
        );
    }
    Ok(())
}
