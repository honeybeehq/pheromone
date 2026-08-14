//! The metric condition engine (ARCHITECTURE: "metrics never enter as raw
//! datapoints"). Conditions are evaluated here, at the tap edge; only
//! condition *transitions* (`metric.condition.entered` / `.cleared`) become
//! trail events. The raw firehose never hits the matcher or the log.
//!
//! Semantics: a condition ENTERS when its predicate holds continuously for
//! `hold` (measured across received datapoints), and CLEARS on the first
//! datapoint where the predicate fails. Statefulness lives here, not in the
//! matcher (AGENTS.md principle 3).

use std::collections::HashMap;

use pher_core::Dur;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    Gt,
    Lt,
    Ge,
    Le,
}

impl Op {
    fn holds(&self, value: f64, threshold: f64) -> bool {
        match self {
            Op::Gt => value > threshold,
            Op::Lt => value < threshold,
            Op::Ge => value >= threshold,
            Op::Le => value <= threshold,
        }
    }

    fn sym(&self) -> &'static str {
        match self {
            Op::Gt => ">",
            Op::Lt => "<",
            Op::Ge => ">=",
            Op::Le => "<=",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionDef {
    pub name: String,
    /// Metric name the condition watches (exact match).
    pub metric: String,
    /// Labels that must all be present on a datapoint for it to count.
    #[serde(default)]
    pub labels: HashMap<String, String>,
    pub op: Op,
    pub threshold: f64,
    /// Predicate must hold continuously this long before entering.
    pub hold: Dur,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConditionState {
    pub active: bool,
    /// Unix secs when the predicate started holding (while inactive).
    #[serde(rename = "pendingSince", skip_serializing_if = "Option::is_none")]
    pub pending_since: Option<u64>,
    #[serde(rename = "lastValue")]
    pub last_value: f64,
}

/// A transition to be emitted onto the trail.
#[derive(Debug, Clone, PartialEq)]
pub struct Transition {
    pub condition: String,
    pub entered: bool, // false = cleared
    pub value: f64,
}

#[derive(Debug, Default)]
pub struct Conditions {
    pub defs: Vec<ConditionDef>,
    pub states: HashMap<String, ConditionState>,
}

impl Conditions {
    pub fn from_json(defs: Value, states: Value) -> Conditions {
        Conditions {
            defs: serde_json::from_value(defs).unwrap_or_default(),
            states: serde_json::from_value(states).unwrap_or_default(),
        }
    }

    pub fn add(&mut self, def: ConditionDef) -> Result<(), String> {
        if self.defs.iter().any(|d| d.name == def.name) {
            return Err(format!("condition '{}' already exists", def.name));
        }
        self.defs.push(def);
        Ok(())
    }

    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.defs.len();
        self.defs.retain(|d| d.name != name);
        self.states.remove(name);
        before != self.defs.len()
    }

    /// Feed one datapoint; returns transitions to emit. Datapoints that match
    /// no condition are dropped entirely — that is the design.
    pub fn ingest(
        &mut self,
        metric: &str,
        value: f64,
        labels: &HashMap<String, String>,
        now: u64,
    ) -> Vec<Transition> {
        let mut out = Vec::new();
        for def in &self.defs {
            if def.metric != metric {
                continue;
            }
            if !def.labels.iter().all(|(k, v)| labels.get(k) == Some(v)) {
                continue;
            }
            let state = self.states.entry(def.name.clone()).or_default();
            state.last_value = value;
            if def.op.holds(value, def.threshold) {
                if state.active {
                    continue; // still inside the condition
                }
                let since = *state.pending_since.get_or_insert(now);
                if now.saturating_sub(since) >= def.hold.secs() {
                    state.active = true;
                    state.pending_since = None;
                    out.push(Transition {
                        condition: def.name.clone(),
                        entered: true,
                        value,
                    });
                }
            } else {
                state.pending_since = None;
                if state.active {
                    state.active = false;
                    out.push(Transition {
                        condition: def.name.clone(),
                        entered: false,
                        value,
                    });
                }
            }
        }
        out
    }

    pub fn def(&self, name: &str) -> Option<&ConditionDef> {
        self.defs.iter().find(|d| d.name == name)
    }

    /// The trail event payload for a transition.
    pub fn event_payload(&self, t: &Transition) -> Value {
        let def = self.def(&t.condition);
        json!({
            "name": t.condition,
            "value": t.value,
            "metric": def.map(|d| d.metric.clone()),
            "threshold": def.map(|d| d.threshold),
            "op": def.map(|d| d.op.sym()),
            "labels": def.map(|d| d.labels.clone()),
            "hold": def.map(|d| d.hold.text().to_string()),
        })
    }

    pub fn status(&self) -> Value {
        json!(self
            .defs
            .iter()
            .map(|d| {
                let s = self.states.get(&d.name).cloned().unwrap_or_default();
                json!({
                    "name": d.name,
                    "metric": d.metric,
                    "op": d.op.sym(),
                    "threshold": d.threshold,
                    "hold": d.hold.text(),
                    "labels": d.labels,
                    "active": s.active,
                    "lastValue": s.last_value,
                })
            })
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cond(hold: &str) -> ConditionDef {
        ConditionDef {
            name: "p95_high".into(),
            metric: "p95_latency".into(),
            labels: HashMap::from([("env".to_string(), "prod".to_string())]),
            op: Op::Gt,
            threshold: 800.0,
            hold: Dur::parse(hold).unwrap(),
        }
    }

    fn labels(env: &str) -> HashMap<String, String> {
        HashMap::from([("env".to_string(), env.to_string())])
    }

    #[test]
    fn enters_after_hold_and_clears_immediately() {
        let mut c = Conditions::default();
        c.add(cond("5m")).unwrap();

        // Breach starts at t=0: pending, not yet entered.
        assert!(c
            .ingest("p95_latency", 900.0, &labels("prod"), 0)
            .is_empty());
        // Still breaching at t=200s: hold not met.
        assert!(c
            .ingest("p95_latency", 950.0, &labels("prod"), 200)
            .is_empty());
        // t=300s: hold met → entered.
        let t = c.ingest("p95_latency", 970.0, &labels("prod"), 300);
        assert_eq!(t.len(), 1);
        assert!(t[0].entered);
        // Still high: no repeat events.
        assert!(c
            .ingest("p95_latency", 990.0, &labels("prod"), 400)
            .is_empty());
        // Recovery clears immediately.
        let t = c.ingest("p95_latency", 400.0, &labels("prod"), 500);
        assert_eq!(t.len(), 1);
        assert!(!t[0].entered);
    }

    #[test]
    fn flapping_below_hold_never_enters() {
        let mut c = Conditions::default();
        c.add(cond("5m")).unwrap();
        assert!(c
            .ingest("p95_latency", 900.0, &labels("prod"), 0)
            .is_empty());
        // Dips below threshold at t=100 → pending resets.
        assert!(c
            .ingest("p95_latency", 700.0, &labels("prod"), 100)
            .is_empty());
        // Breaches again; the clock restarted, so t=350 is only 250s in.
        assert!(c
            .ingest("p95_latency", 900.0, &labels("prod"), 150)
            .is_empty());
        assert!(c
            .ingest("p95_latency", 900.0, &labels("prod"), 350)
            .is_empty());
        // 150+300=450 → entered now.
        assert_eq!(
            c.ingest("p95_latency", 900.0, &labels("prod"), 450).len(),
            1
        );
    }

    #[test]
    fn labels_and_metric_must_match() {
        let mut c = Conditions::default();
        c.add(cond("1s")).unwrap();
        // Wrong env: ignored entirely.
        assert!(c.ingest("p95_latency", 900.0, &labels("dev"), 0).is_empty());
        assert!(c
            .ingest("p95_latency", 900.0, &labels("dev"), 100)
            .is_empty());
        // Wrong metric: ignored.
        assert!(c.ingest("error_rate", 900.0, &labels("prod"), 0).is_empty());
        // Right metric+labels enters after 1s hold.
        assert!(c
            .ingest("p95_latency", 900.0, &labels("prod"), 200)
            .is_empty());
        assert_eq!(
            c.ingest("p95_latency", 900.0, &labels("prod"), 202).len(),
            1
        );
    }

    #[test]
    fn duplicate_names_rejected() {
        let mut c = Conditions::default();
        c.add(cond("5m")).unwrap();
        assert!(c.add(cond("5m")).is_err());
        assert!(c.remove("p95_high"));
        assert!(!c.remove("p95_high"));
    }
}
