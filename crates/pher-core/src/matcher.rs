use serde::Serialize;
use serde_json::Value;

use std::collections::HashMap;

use crate::envelope::Envelope;
use crate::expr::{eval_bool, BoundPath, EvalCtx, Expr, Field, PathCache};
use crate::subject::SubjectTrie;
use crate::subscription::Subscription;

/// The `match` explanation block attached to every delivery.
#[derive(Debug, Clone, Serialize)]
pub struct MatchBlock {
    pub subscription: String,
    /// Tiers that evaluated and passed, in cascade order.
    pub tiers: Vec<String>,
    #[serde(rename = "where")]
    pub where_rec: Option<WhereRecord>,
    /// Tier 3/4 verdicts. Not evaluated in this slice — always null, with
    /// `pending` listing what a full build would still run.
    pub meaning: Option<Value>,
    pub judge: Option<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pending: Vec<String>,
    #[serde(rename = "deliveryId", skip_serializing_if = "Option::is_none")]
    pub delivery_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WhereRecord {
    pub expr: String,
    pub result: bool,
}

/// Outcome of running one event through one subscription's cascade.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    /// All evaluable tiers passed and no tier 3/4 clauses exist.
    Matched,
    /// Tiers 1–2 passed but the subscription has `meaning`/`judge` clauses,
    /// which this build cannot evaluate. Never delivered as a match.
    PendingSemantic { pending: Vec<String> },
    /// A tier rejected the event. `tier` is the first rejecting tier.
    Rejected { tier: String, reason: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct Evaluation {
    #[serde(rename = "subscription")]
    pub sub_id: String,
    #[serde(flatten)]
    pub outcome: Outcome,
    #[serde(rename = "match", skip_serializing_if = "Option::is_none")]
    pub match_block: Option<MatchBlock>,
}

/// Allocation-free cascade outcome for the hot ingest path. Explanations
/// (strings) are built separately by `evaluate` — only for deliveries and
/// introspection, never per-candidate at ingest.
#[derive(Debug, Clone, PartialEq)]
pub enum Check {
    Matched,
    /// Tiers 1–2 passed but meaning/judge exist and cannot run in this build.
    Pending,
    RejectOn,
    RejectFrom,
    RejectWhere,
}

/// Cheap tier 1–2 cascade: no allocation on the match path.
pub fn check(sub: &Subscription, event: &Envelope, origin: Option<&Envelope>) -> Check {
    if !sub.on.iter().any(|p| p.matches(&event.subject)) {
        return Check::RejectOn;
    }
    check_after_subject(sub, &EvalCtx::new(event, origin))
}

/// Tiers 1b–2 only — for candidates that already passed the subject trie,
/// where re-verifying `on` would be pure waste.
fn check_after_subject<'a>(sub: &'a Subscription, ctx: &EvalCtx<'a>) -> Check {
    if let Some(from) = &sub.from {
        if !from.matches(&ctx.event.source) && !from.matches(&ctx.event.node) {
            return Check::RejectFrom;
        }
    }
    if let Some(expr) = &sub.where_expr {
        if !matches!(eval_bool(expr, ctx), Ok(true)) {
            return Check::RejectWhere;
        }
    }
    if sub.meaning.is_some() || sub.judge.is_some() {
        return Check::Pending;
    }
    Check::Matched
}

/// Run one event through one subscription's tier 1–2 cascade, building the
/// full match-explanation record. Use for deliveries, `test`, and `why-not`;
/// use `check` in per-event hot loops.
/// `origin` binds `$origin` (only meaningful for expect-join expressions).
pub fn evaluate(
    sub_id: &str,
    sub: &Subscription,
    event: &Envelope,
    origin: Option<&Envelope>,
) -> Evaluation {
    let mut tiers = Vec::new();

    // Tier 1a — on (subject).
    if !sub.on.iter().any(|p| p.matches(&event.subject)) {
        return rejected(
            sub_id,
            "on",
            format!(
                "subject '{}' matches none of [{}]",
                event.subject,
                sub.on
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
    }
    tiers.push("on".to_string());

    // Tier 1b — from (source or node).
    if let Some(from) = &sub.from {
        if !from.matches(&event.source) && !from.matches(&event.node) {
            return rejected(
                sub_id,
                "from",
                format!(
                    "neither source '{}' nor node '{}' matches '{from}'",
                    event.source, event.node
                ),
            );
        }
        tiers.push("from".to_string());
    }

    // Tier 2 — where.
    let mut where_rec = None;
    if let Some(expr) = &sub.where_expr {
        let ctx = EvalCtx::new(event, origin);
        match eval_bool(expr, &ctx) {
            Ok(true) => {
                where_rec = Some(WhereRecord {
                    expr: expr.canon(),
                    result: true,
                });
                tiers.push("where".to_string());
            }
            Ok(false) => {
                return rejected(
                    sub_id,
                    "where",
                    format!("expression '{}' evaluated to false", expr.canon()),
                );
            }
            Err(e) => {
                return rejected(
                    sub_id,
                    "where",
                    format!("expression '{}' errored: {e}", expr.canon()),
                );
            }
        }
    }

    // Tiers 3–4 — parsed but not evaluable in this slice. Fail open is not an
    // option (that would be a silent lie); report pending instead.
    let mut pending = Vec::new();
    if sub.meaning.is_some() {
        pending.push("meaning".to_string());
    }
    if sub.judge.is_some() {
        pending.push("judge".to_string());
    }

    let match_block = MatchBlock {
        subscription: sub_id.to_string(),
        tiers,
        where_rec,
        meaning: None,
        judge: None,
        pending: pending.clone(),
        delivery_id: None,
    };
    if pending.is_empty() {
        Evaluation {
            sub_id: sub_id.to_string(),
            outcome: Outcome::Matched,
            match_block: Some(match_block),
        }
    } else {
        Evaluation {
            sub_id: sub_id.to_string(),
            outcome: Outcome::PendingSemantic { pending },
            match_block: Some(match_block),
        }
    }
}

fn rejected(sub_id: &str, tier: &str, reason: String) -> Evaluation {
    Evaluation {
        sub_id: sub_id.to_string(),
        outcome: Outcome::Rejected {
            tier: tier.to_string(),
            reason,
        },
        match_block: None,
    }
}

struct Entry {
    id: String,
    sub: Subscription,
}

/// A set of registered subscriptions with a subject-trie prefilter and a
/// path interner: every distinct non-`$origin` where-path gets a slot so it
/// resolves at most once per event across all candidates.
#[derive(Default)]
pub struct Matcher {
    entries: Vec<Entry>,
    trie: SubjectTrie<usize>,
    path_slots: HashMap<(Field, Vec<String>), u32>,
}

impl Matcher {
    pub fn new() -> Self {
        Matcher::default()
    }

    pub fn insert(&mut self, id: impl Into<String>, mut sub: Subscription) {
        if let Some(expr) = &mut sub.where_expr {
            assign_slots(expr, &mut self.path_slots);
        }
        let idx = self.entries.len();
        for p in &sub.on {
            self.trie.insert(p, idx);
        }
        self.entries.push(Entry { id: id.into(), sub });
    }

    pub fn remove(&mut self, id: &str) -> bool {
        let before = self.entries.len();
        let entries = std::mem::take(&mut self.entries);
        let kept: Vec<Entry> = entries.into_iter().filter(|e| e.id != id).collect();
        self.trie = SubjectTrie::new();
        for (idx, e) in kept.iter().enumerate() {
            for p in &e.sub.on {
                self.trie.insert(p, idx);
            }
        }
        self.entries = kept;
        before != self.entries.len()
    }

    pub fn get(&self, id: &str) -> Option<&Subscription> {
        self.entries.iter().find(|e| e.id == id).map(|e| &e.sub)
    }

    pub fn ids(&self) -> Vec<&str> {
        self.entries.iter().map(|e| e.id.as_str()).collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Trie-prefiltered candidates, then full cascade per candidate.
    /// Returns evaluations for candidates only (subject-miss subscriptions are
    /// never touched — that is the point of the trie).
    pub fn evaluate_event(&self, event: &Envelope) -> Vec<Evaluation> {
        self.candidate_idxs(event)
            .iter()
            .map(|&i| {
                let e = &self.entries[i];
                evaluate(&e.id, &e.sub, event, None)
            })
            .collect()
    }

    /// Hot path: ids of subscriptions whose tier 1–2 cascade passes, with no
    /// explanation allocation for the (vast) non-matching majority.
    /// Convenience wrapper; sustained ingest loops should hold a [`Scratch`]
    /// and call [`Matcher::match_ids_with`].
    pub fn match_ids(&self, event: &Envelope) -> Vec<&str> {
        let mut scratch = Scratch::new();
        let mut out = Vec::new();
        self.match_ids_with(event, &mut scratch, &mut out);
        out
    }

    /// Zero-allocation-per-event matching: candidates are deduplicated with an
    /// epoch-stamped seen table and both buffers are reused across calls.
    pub fn match_ids_with<'m>(
        &'m self,
        event: &Envelope,
        scratch: &mut Scratch,
        out: &mut Vec<&'m str>,
    ) {
        out.clear();
        scratch.begin(self.entries.len());
        let epoch = scratch.epoch;
        scratch.idxs.clear();
        {
            let idxs = &mut scratch.idxs;
            let seen = &mut scratch.seen;
            self.trie.for_each_match(&event.subject, |&i| {
                if seen[i] != epoch {
                    seen[i] = epoch;
                    idxs.push(i);
                }
            });
        }
        let cache = PathCache::new();
        let mut ctx = EvalCtx::new(event, None);
        ctx.cache = Some(&cache);
        for &i in &scratch.idxs {
            let e = &self.entries[i];
            if check_after_subject(&e.sub, &ctx) == Check::Matched {
                out.push(e.id.as_str());
            }
        }
    }

    fn candidate_idxs(&self, event: &Envelope) -> Vec<usize> {
        let mut idxs: Vec<usize> = self
            .trie
            .matches(&event.subject)
            .into_iter()
            .copied()
            .collect();
        idxs.sort_unstable();
        idxs.dedup();
        idxs
    }
}

/// Reusable per-consumer buffers for [`Matcher::match_ids_with`].
#[derive(Default)]
pub struct Scratch {
    idxs: Vec<usize>,
    seen: Vec<u32>,
    epoch: u32,
}

impl Scratch {
    pub fn new() -> Self {
        Scratch::default()
    }

    fn begin(&mut self, entries: usize) {
        if self.seen.len() < entries {
            self.seen.resize(entries, 0);
        }
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            // Wrapped: clear stale stamps and restart at 1.
            self.seen.fill(0);
            self.epoch = 1;
        }
    }
}

/// Walk a where-expression and assign interned slots to every cacheable
/// (non-`$origin`) path.
fn assign_slots(expr: &mut Expr, interner: &mut HashMap<(Field, Vec<String>), u32>) {
    let mut slot_of = |bp: &mut BoundPath| {
        if bp.origin {
            return; // origin-relative paths vary per timer; never cached
        }
        let next = interner.len() as u32;
        let id = *interner.entry((bp.field, bp.segs.clone())).or_insert(next);
        bp.slot = Some(id);
    };
    match expr {
        Expr::Path(bp) | Expr::Has(bp) => slot_of(bp),
        Expr::Not(e) | Expr::Size(e) => assign_slots(e, interner),
        Expr::And(a, b) | Expr::Or(a, b) | Expr::In(a, b) => {
            assign_slots(a, interner);
            assign_slots(b, interner);
        }
        Expr::Cmp(_, a, b) => {
            assign_slots(a, interner);
            assign_slots(b, interner);
        }
        Expr::Matches(a, _) => assign_slots(a, interner),
        Expr::List(items) => {
            for e in items {
                assign_slots(e, interner);
            }
        }
        Expr::Lit(_) => {}
    }
}

/// One-command diagnosis of a silent subscription: replay one event through one
/// subscription and report the first tier that rejected it (or the match).
pub fn why_not(sub_id: &str, sub: &Subscription, event: &Envelope) -> Value {
    let eval = evaluate(sub_id, sub, event, None);
    match &eval.outcome {
        Outcome::Rejected { tier, reason } => serde_json::json!({
            "subscription": sub_id,
            "event": event.id,
            "matched": false,
            "rejectedAt": tier,
            "reason": reason,
        }),
        Outcome::PendingSemantic { pending } => serde_json::json!({
            "subscription": sub_id,
            "event": event.id,
            "matched": false,
            "rejectedAt": null,
            "reason": format!(
                "passed tiers 1-2; {} not evaluated in this build (slice 4/5)",
                pending.join("+")
            ),
        }),
        Outcome::Matched => serde_json::json!({
            "subscription": sub_id,
            "event": event.id,
            "matched": true,
            "match": eval.match_block,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(subject: &str, payload: Value) -> Envelope {
        let mut e = Envelope::new(subject, payload);
        e.id = "PH.ev01".into();
        e.ts = "2026-08-09T12:00:00Z".into();
        e.node = "trmd-mbp".into();
        e.source = "tap.test".into();
        e
    }

    #[test]
    fn cascade_stops_at_first_rejecting_tier() {
        let sub = Subscription::parse(
            r#"on hive.seal where payload.status == "blocked" then cmd echo hit"#,
        )
        .unwrap();

        let e = event("pol.job.done", json!({}));
        let eval = evaluate("S1", &sub, &e, None);
        assert!(matches!(eval.outcome, Outcome::Rejected { ref tier, .. } if tier == "on"));

        let e = event("hive.seal", json!({"status": "done"}));
        let eval = evaluate("S1", &sub, &e, None);
        assert!(matches!(eval.outcome, Outcome::Rejected { ref tier, .. } if tier == "where"));

        let e = event("hive.seal", json!({"status": "blocked"}));
        let eval = evaluate("S1", &sub, &e, None);
        assert!(matches!(eval.outcome, Outcome::Matched));
        let mb = eval.match_block.unwrap();
        assert_eq!(mb.tiers, vec!["on", "where"]);
        assert!(mb.where_rec.unwrap().result);
    }

    #[test]
    fn from_filters_source_or_node() {
        let sub = Subscription::parse("on hive.seal from tap.hive then cmd echo hi").unwrap();
        let mut e = event("hive.seal", json!({}));
        assert!(matches!(
            evaluate("S", &sub, &e, None).outcome,
            Outcome::Rejected { ref tier, .. } if tier == "from"
        ));
        e.source = "tap.hive".into();
        assert!(matches!(
            evaluate("S", &sub, &e, None).outcome,
            Outcome::Matched
        ));
    }

    #[test]
    fn semantic_subscriptions_never_silently_match() {
        let sub = Subscription::parse(
            r#"on ci.github.run.completed meaning "infra flake" > 0.8 then cmd echo hi"#,
        )
        .unwrap();
        let e = event("ci.github.run.completed", json!({}));
        let eval = evaluate("S", &sub, &e, None);
        assert!(
            matches!(eval.outcome, Outcome::PendingSemantic { ref pending } if pending == &vec!["meaning".to_string()])
        );
    }

    #[test]
    fn matcher_uses_trie_prefilter() {
        let mut m = Matcher::new();
        m.insert(
            "A",
            Subscription::parse("on hive.** then cmd echo a").unwrap(),
        );
        m.insert(
            "B",
            Subscription::parse("on pol.job.* then cmd echo b").unwrap(),
        );
        let evals = m.evaluate_event(&event("hive.seal", json!({})));
        assert_eq!(evals.len(), 1);
        assert_eq!(evals[0].sub_id, "A");
        assert!(m.remove("A"));
        assert!(m.evaluate_event(&event("hive.seal", json!({}))).is_empty());
    }

    #[test]
    fn why_not_reports_first_rejection() {
        let sub = Subscription::parse(
            r#"on hive.seal where payload.status == "blocked" then cmd echo hi"#,
        )
        .unwrap();
        let e = event("hive.seal", json!({"status": "open"}));
        let report = why_not("S1", &sub, &e);
        assert_eq!(report["matched"], json!(false));
        assert_eq!(report["rejectedAt"], json!("where"));
    }
}
