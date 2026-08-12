// One-off: measure the real meaning-tier scores used on the pher.dev landing
// page (hero pass + why-not reject). The site shows these as "precomputed".
use pher_core::Envelope;
use serde_json::json;

fn main() -> anyhow::Result<()> {
    let e = pher_embed::Embedder::new(dirs_path().unwrap_or_else(|| "/tmp/pher-models".into()))?;
    let descriptor = "agent stuck on auth or credentials";
    let d = e.embed_one(descriptor)?;

    let pass = Envelope::new(
        "hive.seal",
        json!({"status": "blocked", "note": "oauth token expired, 401 unauthorized on credential refresh, retries exhausted", "bee": "CL.6308"}),
    );
    let reject_review = Envelope::new(
        "hive.seal",
        json!({"status": "blocked", "note": "waiting for code review approval from operator", "bee": "CL.6308"}),
    );
    let reject_disk = Envelope::new(
        "hive.seal",
        json!({"status": "blocked", "note": "no space left on device during artifact upload", "bee": "CL.6308"}),
    );
    let reject_merge = Envelope::new(
        "hive.seal",
        json!({"status": "blocked", "note": "merge conflict in lockfile, needs manual rebase", "bee": "CL.6308"}),
    );
    for (name, env) in [
        ("hero-pass", &pass),
        ("reject-review", &reject_review),
        ("reject-disk", &reject_disk),
        ("reject-merge", &reject_merge),
    ] {
        let v = e.embed_one(&pher_embed::project(env))?;
        println!("{name}: {:.4}", pher_embed::cosine(&d, &v));
    }
    Ok(())
}

fn dirs_path() -> Option<std::path::PathBuf> {
    let home = std::env::var("HOME").ok()?;
    let p = std::path::PathBuf::from(home).join(".pheromone/models");
    p.exists().then_some(p)
}
