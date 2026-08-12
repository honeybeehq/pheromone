use std::path::{Path, PathBuf};

/// State-directory layout: `$PHEROMONE_HOME` or `~/.pheromone`.
#[derive(Debug, Clone)]
pub struct Paths {
    pub home: PathBuf,
}

impl Paths {
    pub fn resolve() -> Paths {
        let home = std::env::var_os("PHEROMONE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let base = std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .unwrap_or_default();
                base.join(".pheromone")
            });
        Paths { home }
    }

    pub fn ensure(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.home)
    }

    pub fn sock(&self) -> PathBuf {
        self.home.join("pherd.sock")
    }

    pub fn events(&self) -> PathBuf {
        self.home.join("events.jsonl")
    }

    pub fn subs(&self) -> PathBuf {
        self.home.join("subs.json")
    }

    pub fn deliveries(&self) -> PathBuf {
        self.home.join("deliveries.jsonl")
    }

    pub fn timers(&self) -> PathBuf {
        self.home.join("timers.json")
    }

    pub fn pending(&self) -> PathBuf {
        self.home.join("pending.json")
    }

    pub fn vectors(&self) -> PathBuf {
        self.home.join("vectors.jsonl")
    }

    pub fn models(&self) -> PathBuf {
        self.home.join("models")
    }

    pub fn verdicts(&self) -> PathBuf {
        self.home.join("verdicts.jsonl")
    }

    pub fn judge_budgets(&self) -> PathBuf {
        self.home.join("judge_budgets.json")
    }

    pub fn conditions(&self) -> PathBuf {
        self.home.join("conditions.json")
    }

    pub fn condition_state(&self) -> PathBuf {
        self.home.join("condition_state.json")
    }

    pub fn cursors(&self) -> PathBuf {
        self.home.join("cursors.json")
    }

    pub fn outbox(&self) -> PathBuf {
        self.home.join("outbox.json")
    }

    pub fn forwarded(&self) -> PathBuf {
        self.home.join("forwarded_seen.jsonl")
    }

    pub fn bridges(&self) -> PathBuf {
        self.home.join("bridges.json")
    }

    pub fn grants(&self) -> PathBuf {
        self.home.join("grants.json")
    }
}

/// Atomic-ish JSON write: temp file + rename, so kill -9 never leaves a torn file.
pub fn write_json_atomic(path: &Path, value: &serde_json::Value) -> anyhow::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
