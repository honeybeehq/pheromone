use std::collections::HashMap;

use crate::lexer::{Cursor, Tok};
use crate::ParseError;

/// One token of a subject pattern.
#[derive(Debug, Clone, PartialEq)]
pub enum PatTok {
    Lit(String),
    /// `*` — exactly one token.
    Star,
    /// `**` — one or more remaining tokens; terminal position only (NATS `>` semantics).
    DStar,
}

/// A dot-separated subject pattern with `*`/`**` wildcards, e.g. `hive.bee.*`.
#[derive(Debug, Clone, PartialEq)]
pub struct SubjectPattern {
    toks: Vec<PatTok>,
}

fn valid_subject_token(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

impl SubjectPattern {
    pub fn parse(text: &str) -> Result<Self, ParseError> {
        let mut toks = Vec::new();
        for (i, part) in text.split('.').enumerate() {
            match part {
                "*" => toks.push(PatTok::Star),
                "**" => {
                    toks.push(PatTok::DStar);
                    if text.split('.').count() != i + 1 {
                        return Err(ParseError::new(format!(
                            "'**' must be the last token in subject pattern '{text}'"
                        )));
                    }
                }
                lit if valid_subject_token(lit) => toks.push(PatTok::Lit(lit.to_string())),
                bad => {
                    return Err(ParseError::new(format!(
                        "invalid subject token '{bad}' in pattern '{text}'"
                    )))
                }
            }
        }
        if toks.is_empty() {
            return Err(ParseError::new("empty subject pattern"));
        }
        Ok(SubjectPattern { toks })
    }

    /// Parse a subject pattern from a clause token stream (`hive.bee.*` lexes as
    /// Ident/Punct('.')/Op("*") sequences).
    pub fn parse_tokens(cur: &mut Cursor) -> Result<Self, ParseError> {
        let mut toks = Vec::new();
        loop {
            match cur.next() {
                Some(Tok::Ident(s)) => toks.push(PatTok::Lit(s.clone())),
                Some(Tok::Num(n)) if n.fract() == 0.0 && *n >= 0.0 => {
                    toks.push(PatTok::Lit(format!("{}", *n as u64)))
                }
                Some(Tok::Op("*")) => toks.push(PatTok::Star),
                Some(Tok::Op("**")) => {
                    toks.push(PatTok::DStar);
                    if cur.peek() == Some(&Tok::Punct('.')) {
                        return Err(ParseError::new("'**' must be the last token in a subject"));
                    }
                    break;
                }
                other => {
                    return Err(ParseError::new(format!(
                        "expected subject token, found {}",
                        other.map_or("end of input".to_string(), |t| format!("'{t}'"))
                    )))
                }
            }
            if !cur.eat_punct('.') {
                break;
            }
        }
        Ok(SubjectPattern { toks })
    }

    pub fn is_concrete(&self) -> bool {
        self.toks.iter().all(|t| matches!(t, PatTok::Lit(_)))
    }

    pub fn matches(&self, subject: &str) -> bool {
        let mut parts = [""; 16];
        let mut n = 0;
        for part in subject.split('.') {
            if n == parts.len() {
                let all: Vec<&str> = subject.split('.').collect();
                return Self::match_toks(&self.toks, &all);
            }
            parts[n] = part;
            n += 1;
        }
        Self::match_toks(&self.toks, &parts[..n])
    }

    fn match_toks(toks: &[PatTok], parts: &[&str]) -> bool {
        match (toks.first(), parts.first()) {
            (None, None) => true,
            (Some(PatTok::DStar), Some(_)) => true, // one-or-more remainder
            (Some(PatTok::Star), Some(_)) => Self::match_toks(&toks[1..], &parts[1..]),
            (Some(PatTok::Lit(l)), Some(p)) if l == p => Self::match_toks(&toks[1..], &parts[1..]),
            _ => false,
        }
    }

    pub fn tokens(&self) -> &[PatTok] {
        &self.toks
    }
}

impl std::fmt::Display for SubjectPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self
            .toks
            .iter()
            .map(|t| match t {
                PatTok::Lit(s) => s.clone(),
                PatTok::Star => "*".to_string(),
                PatTok::DStar => "**".to_string(),
            })
            .collect();
        write!(f, "{}", parts.join("."))
    }
}

#[derive(Debug, Default)]
struct TrieNode<T> {
    children: HashMap<String, TrieNode<T>>,
    star: Option<Box<TrieNode<T>>>,
    /// Values whose pattern ends in `**` at this depth.
    dstar_vals: Vec<T>,
    /// Values whose pattern ends exactly here.
    vals: Vec<T>,
}

impl<T> TrieNode<T> {
    fn new() -> Self {
        TrieNode {
            children: HashMap::new(),
            star: None,
            dstar_vals: Vec::new(),
            vals: Vec::new(),
        }
    }
}

/// Subject index: pattern → values, matched against concrete subjects at ingest.
#[derive(Debug)]
pub struct SubjectTrie<T> {
    root: TrieNode<T>,
}

impl<T> Default for SubjectTrie<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SubjectTrie<T> {
    pub fn new() -> Self {
        SubjectTrie {
            root: TrieNode::new(),
        }
    }

    pub fn insert(&mut self, pattern: &SubjectPattern, val: T) {
        let mut node = &mut self.root;
        for tok in pattern.tokens() {
            match tok {
                PatTok::Lit(s) => {
                    node = node.children.entry(s.clone()).or_insert_with(TrieNode::new);
                }
                PatTok::Star => {
                    node = node.star.get_or_insert_with(|| Box::new(TrieNode::new()));
                }
                PatTok::DStar => {
                    node.dstar_vals.push(val);
                    return;
                }
            }
        }
        node.vals.push(val);
    }

    /// Collect all values whose pattern matches the concrete subject.
    pub fn matches(&self, subject: &str) -> Vec<&T> {
        let mut out = Vec::new();
        self.for_each_match(subject, |v| out.push(v));
        out
    }

    /// Visit every value whose pattern matches, without allocating a result
    /// vector. The hot ingest path uses this with caller-owned scratch.
    pub fn for_each_match<'a>(&'a self, subject: &str, mut f: impl FnMut(&'a T)) {
        let mut parts = [""; 16];
        let mut n = 0;
        for part in subject.split('.') {
            if n == parts.len() {
                // Absurdly deep subject: fall back to the allocating path.
                let all: Vec<&str> = subject.split('.').collect();
                Self::visit(&self.root, &all, &mut f);
                return;
            }
            parts[n] = part;
            n += 1;
        }
        Self::visit(&self.root, &parts[..n], &mut f);
    }

    fn visit<'a>(node: &'a TrieNode<T>, parts: &[&str], f: &mut impl FnMut(&'a T)) {
        match parts.first() {
            None => {
                for v in &node.vals {
                    f(v);
                }
            }
            Some(part) => {
                // `**` here consumes the (non-empty) remainder.
                for v in &node.dstar_vals {
                    f(v);
                }
                if let Some(child) = node.children.get(*part) {
                    Self::visit(child, &parts[1..], f);
                }
                if let Some(star) = &node.star {
                    Self::visit(star, &parts[1..], f);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_matching() {
        let p = SubjectPattern::parse("hive.bee.*").unwrap();
        assert!(p.matches("hive.bee.spawned"));
        assert!(!p.matches("hive.bee"));
        assert!(!p.matches("hive.bee.spawned.extra"));

        let p = SubjectPattern::parse("hive.**").unwrap();
        assert!(p.matches("hive.seal"));
        assert!(p.matches("hive.flight.landed.hard"));
        assert!(!p.matches("hive"), "** requires at least one token");
        assert!(!p.matches("pol.job.done"));

        let p = SubjectPattern::parse("ci.github.run.completed").unwrap();
        assert!(p.matches("ci.github.run.completed"));
        assert!(!p.matches("ci.github.run.started"));
    }

    #[test]
    fn dstar_terminal_only() {
        assert!(SubjectPattern::parse("hive.**.seal").is_err());
        assert!(SubjectPattern::parse("**").is_ok());
    }

    #[test]
    fn trie_collects_all_matches() {
        let mut trie = SubjectTrie::new();
        trie.insert(&SubjectPattern::parse("hive.seal").unwrap(), 1);
        trie.insert(&SubjectPattern::parse("hive.*").unwrap(), 2);
        trie.insert(&SubjectPattern::parse("hive.**").unwrap(), 3);
        trie.insert(&SubjectPattern::parse("pol.job.*").unwrap(), 4);
        trie.insert(&SubjectPattern::parse("**").unwrap(), 5);

        let mut hits: Vec<i32> = trie.matches("hive.seal").into_iter().copied().collect();
        hits.sort();
        assert_eq!(hits, vec![1, 2, 3, 5]);

        let mut hits: Vec<i32> = trie
            .matches("hive.flight.landed")
            .into_iter()
            .copied()
            .collect();
        hits.sort();
        assert_eq!(hits, vec![3, 5]);

        let hits: Vec<i32> = trie.matches("pol.job.done").into_iter().copied().collect();
        assert!(hits.contains(&4) && hits.contains(&5));
    }
}
