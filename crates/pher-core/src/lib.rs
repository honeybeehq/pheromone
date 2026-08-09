//! pher-core — the Pheromone matching core.
//!
//! Envelope, subject trie, subscription language (parser/formatter/canonical JSON),
//! `where` evaluator, and the tier 1–2 matcher cascade with match explanations.
//!
//! Tiers 3 (`meaning`) and 4 (`judge`) are parsed and represented but not evaluated
//! in this slice; the matcher reports them as pending, never as silently matched.

pub mod duration;
pub mod envelope;
pub mod expr;
pub mod ids;
pub mod lexer;
pub mod matcher;
pub mod subject;
pub mod subscription;

pub use duration::Dur;
pub use envelope::Envelope;
pub use expr::{EvalCtx, EvalError, Expr};
pub use matcher::{Evaluation, MatchBlock, Matcher, Outcome};
pub use subject::{SubjectPattern, SubjectTrie};
pub use subscription::{
    Action, Delivery, Judge, Lifetime, Meaning, MeaningKind, Replay, Sink, Subscription,
};

/// Errors produced while parsing the subscription language.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ParseError(pub String);

impl ParseError {
    pub fn new(msg: impl Into<String>) -> Self {
        ParseError(msg.into())
    }
}
