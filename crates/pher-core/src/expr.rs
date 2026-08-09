use std::borrow::Cow;

use serde_json::Value;

use crate::envelope::Envelope;
use crate::lexer::{lex, quote, Cursor, Tok};
use crate::ParseError;

/// The `where` expression AST — the documented CEL-syntax subset.
///
/// Supported: `== != < <= > >=` · `&& || !` · `in` · `matches` (RE2-class) ·
/// `has(path)` · `size(x)` · string/number/bool/null/list literals · parens.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Lit(Value),
    List(Vec<Expr>),
    /// Dotted path; root resolved at parse time (`payload`, `type`, …, `$origin`).
    Path(BoundPath),
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Cmp(CmpOp, Box<Expr>, Box<Expr>),
    In(Box<Expr>, Box<Expr>),
    /// Pattern compiled at parse time and cached for the hot path.
    Matches(Box<Expr>, CachedRegex),
    Has(BoundPath),
    Size(Box<Expr>),
}

/// An envelope field a path can be rooted at. Resolved at parse time so the
/// per-event hot loop never does root-name string comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Field {
    Type,
    Subject,
    Source,
    Node,
    Ts,
    Correlation,
    Payload,
}

impl Field {
    fn parse(s: &str) -> Option<Field> {
        Some(match s {
            "type" => Field::Type,
            "subject" => Field::Subject,
            "source" => Field::Source,
            "node" => Field::Node,
            "ts" => Field::Ts,
            "correlation" => Field::Correlation,
            "payload" => Field::Payload,
            _ => return None,
        })
    }

    fn name(&self) -> &'static str {
        match self {
            Field::Type => "type",
            Field::Subject => "subject",
            Field::Source => "source",
            Field::Node => "node",
            Field::Ts => "ts",
            Field::Correlation => "correlation",
            Field::Payload => "payload",
        }
    }
}

/// A parse-time-resolved dotted path: optional `$origin` prefix, the envelope
/// field, and any remaining segments (meaningful only under `payload`).
#[derive(Debug, Clone)]
pub struct BoundPath {
    pub origin: bool,
    pub field: Field,
    pub segs: Vec<String>,
    /// Per-matcher interned path id, assigned at registration; enables the
    /// resolve-once-per-event cache. Ignored by equality and printing.
    pub slot: Option<u32>,
}

impl PartialEq for BoundPath {
    fn eq(&self, other: &Self) -> bool {
        self.origin == other.origin && self.field == other.field && self.segs == other.segs
    }
}

impl BoundPath {
    fn from_segments(path: Vec<String>) -> Result<BoundPath, ParseError> {
        let (origin, rest) = if path[0] == "$origin" {
            if path.len() < 2 {
                return Err(ParseError::new(
                    "unknown field '$origin' (use $origin.type/.subject/.source/.node/.ts/.correlation/.payload)",
                ));
            }
            (true, &path[1..])
        } else {
            (false, &path[..])
        };
        let field = Field::parse(&rest[0]).ok_or_else(|| {
            if origin {
                ParseError::new(format!(
                    "unknown field '$origin.{}' (use $origin.type/.subject/.source/.node/.ts/.correlation/.payload)",
                    rest[0]
                ))
            } else {
                ParseError::new(format!(
                    "unknown identifier '{}' (bound: type, subject, source, node, ts, correlation, payload, $origin)",
                    rest[0]
                ))
            }
        })?;
        Ok(BoundPath {
            origin,
            field,
            segs: rest[1..].to_vec(),
            slot: None,
        })
    }
}

impl std::fmt::Display for BoundPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.origin {
            write!(f, "$origin.")?;
        }
        write!(f, "{}", self.field.name())?;
        for seg in &self.segs {
            write!(f, ".{seg}")?;
        }
        Ok(())
    }
}

/// A regex validated and compiled once at parse time. Equality is on the
/// pattern text, so the AST stays comparable and round-trippable.
#[derive(Debug, Clone)]
pub struct CachedRegex {
    pattern: String,
    compiled: std::sync::OnceLock<regex::Regex>,
}

impl CachedRegex {
    pub fn new(pattern: &str) -> Result<Self, ParseError> {
        let compiled = std::sync::OnceLock::new();
        let re = regex::Regex::new(pattern)
            .map_err(|e| ParseError::new(format!("invalid regex in 'matches': {e}")))?;
        let _ = compiled.set(re);
        Ok(CachedRegex {
            pattern: pattern.to_string(),
            compiled,
        })
    }

    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    fn regex(&self) -> &regex::Regex {
        self.compiled
            .get_or_init(|| regex::Regex::new(&self.pattern).expect("validated at parse time"))
    }
}

impl PartialEq for CachedRegex {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    fn sym(&self) -> &'static str {
        match self {
            CmpOp::Eq => "==",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        }
    }
}

impl Expr {
    pub fn parse_str(input: &str) -> Result<Expr, ParseError> {
        let toks = lex(input)?;
        let mut cur = Cursor::new(&toks);
        let e = Expr::parse(&mut cur)?;
        if !cur.at_end() {
            return Err(ParseError::new(format!(
                "unexpected trailing input in expression: {}",
                cur.describe_next()
            )));
        }
        Ok(e)
    }

    /// Parse an expression from a token cursor, stopping where it can no longer
    /// extend (e.g. at the next clause keyword).
    pub fn parse(cur: &mut Cursor) -> Result<Expr, ParseError> {
        parse_or(cur)
    }

    /// Canonical string form (double-quoted strings, minimal parens).
    pub fn canon(&self) -> String {
        self.print(0)
    }

    // Precedence: Or=1, And=2, Cmp/In/Matches=3, Not=4, primary=5.
    fn prec(&self) -> u8 {
        match self {
            Expr::Or(..) => 1,
            Expr::And(..) => 2,
            Expr::Cmp(..) | Expr::In(..) | Expr::Matches(..) => 3,
            Expr::Not(..) => 4,
            _ => 5,
        }
    }

    fn print(&self, parent_prec: u8) -> String {
        let s = match self {
            Expr::Lit(v) => print_value(v),
            Expr::List(items) => {
                let inner: Vec<String> = items.iter().map(|e| e.print(0)).collect();
                format!("[{}]", inner.join(", "))
            }
            Expr::Path(p) => p.to_string(),
            Expr::Not(e) => format!("!{}", e.print(4)),
            Expr::And(a, b) => format!("{} && {}", a.print(2), b.print(2)),
            Expr::Or(a, b) => format!("{} || {}", a.print(1), b.print(1)),
            Expr::Cmp(op, a, b) => format!("{} {} {}", a.print(4), op.sym(), b.print(4)),
            Expr::In(a, b) => format!("{} in {}", a.print(4), b.print(4)),
            Expr::Matches(a, re) => format!("{} matches {}", a.print(4), quote(re.pattern())),
            Expr::Has(p) => format!("has({p})"),
            Expr::Size(e) => format!("size({})", e.print(0)),
        };
        if self.prec() < parent_prec {
            format!("({s})")
        } else {
            s
        }
    }
}

fn print_value(v: &Value) -> String {
    match v {
        Value::String(s) => quote(s),
        Value::Number(n) => format!("{n}"),
        Value::Bool(b) => format!("{b}"),
        Value::Null => "null".to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn parse_or(cur: &mut Cursor) -> Result<Expr, ParseError> {
    let mut left = parse_and(cur)?;
    while cur.eat_op("||") {
        let right = parse_and(cur)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and(cur: &mut Cursor) -> Result<Expr, ParseError> {
    let mut left = parse_cmp(cur)?;
    while cur.eat_op("&&") {
        let right = parse_cmp(cur)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_cmp(cur: &mut Cursor) -> Result<Expr, ParseError> {
    let left = parse_unary(cur)?;
    let op = match cur.peek() {
        Some(Tok::Op("==")) => Some(CmpOp::Eq),
        Some(Tok::Op("!=")) => Some(CmpOp::Ne),
        Some(Tok::Op("<")) => Some(CmpOp::Lt),
        Some(Tok::Op("<=")) => Some(CmpOp::Le),
        Some(Tok::Op(">")) => Some(CmpOp::Gt),
        Some(Tok::Op(">=")) => Some(CmpOp::Ge),
        _ => None,
    };
    if let Some(op) = op {
        cur.next();
        let right = parse_unary(cur)?;
        return Ok(Expr::Cmp(op, Box::new(left), Box::new(right)));
    }
    if let Some(Tok::Ident(kw)) = cur.peek() {
        match kw.as_str() {
            "in" => {
                cur.next();
                let right = parse_unary(cur)?;
                return Ok(Expr::In(Box::new(left), Box::new(right)));
            }
            "matches" => {
                cur.next();
                match cur.next() {
                    Some(Tok::Str(pat)) => {
                        return Ok(Expr::Matches(Box::new(left), CachedRegex::new(pat)?));
                    }
                    other => {
                        return Err(ParseError::new(format!(
                            "'matches' requires a string literal pattern, found {}",
                            other.map_or("end of input".to_string(), |t| format!("'{t}'"))
                        )))
                    }
                }
            }
            _ => {}
        }
    }
    Ok(left)
}

fn parse_unary(cur: &mut Cursor) -> Result<Expr, ParseError> {
    if cur.eat_op("!") {
        let inner = parse_unary(cur)?;
        return Ok(Expr::Not(Box::new(inner)));
    }
    parse_primary(cur)
}

fn parse_primary(cur: &mut Cursor) -> Result<Expr, ParseError> {
    match cur.peek() {
        Some(Tok::Num(n)) => {
            let n = *n;
            cur.next();
            Ok(Expr::Lit(number_value(n)))
        }
        Some(Tok::Str(s)) => {
            let s = s.clone();
            cur.next();
            Ok(Expr::Lit(Value::String(s)))
        }
        Some(Tok::Punct('(')) => {
            cur.next();
            let e = parse_or(cur)?;
            cur.expect_punct(')')?;
            Ok(e)
        }
        Some(Tok::Punct('[')) => {
            cur.next();
            let mut items = Vec::new();
            if !cur.eat_punct(']') {
                loop {
                    items.push(parse_or(cur)?);
                    if cur.eat_punct(']') {
                        break;
                    }
                    cur.expect_punct(',')?;
                }
            }
            Ok(Expr::List(items))
        }
        Some(Tok::Ident(id)) => {
            let id = id.clone();
            cur.next();
            match id.as_str() {
                "true" => return Ok(Expr::Lit(Value::Bool(true))),
                "false" => return Ok(Expr::Lit(Value::Bool(false))),
                "null" => return Ok(Expr::Lit(Value::Null)),
                "has" if cur.peek() == Some(&Tok::Punct('(')) => {
                    cur.next();
                    let path = parse_path(cur, &id)?;
                    cur.expect_punct(')')?;
                    return Ok(Expr::Has(path));
                }
                "size" if cur.peek() == Some(&Tok::Punct('(')) => {
                    cur.next();
                    let e = parse_or(cur)?;
                    cur.expect_punct(')')?;
                    return Ok(Expr::Size(Box::new(e)));
                }
                _ => {}
            }
            let mut path = vec![id];
            while cur.peek() == Some(&Tok::Punct('.')) {
                // Only consume the dot if a path segment follows.
                match cur.peek_at(1) {
                    Some(Tok::Ident(_)) | Some(Tok::Num(_)) => {
                        cur.next(); // dot
                        match cur.next().unwrap() {
                            Tok::Ident(s) => path.push(s.clone()),
                            Tok::Num(n) if n.fract() == 0.0 && *n >= 0.0 => {
                                path.push(format!("{}", *n as u64))
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => break,
                }
            }
            Ok(Expr::Path(BoundPath::from_segments(path)?))
        }
        other => Err(ParseError::new(format!(
            "expected expression, found {}",
            other.map_or("end of input".to_string(), |t| format!("'{t}'"))
        ))),
    }
}

fn parse_path(cur: &mut Cursor, func: &str) -> Result<BoundPath, ParseError> {
    let mut path = Vec::new();
    loop {
        match cur.next() {
            Some(Tok::Ident(s)) => path.push(s.clone()),
            Some(Tok::Num(n)) if n.fract() == 0.0 && *n >= 0.0 => {
                path.push(format!("{}", *n as u64))
            }
            other => {
                return Err(ParseError::new(format!(
                    "'{func}' expects a dotted path, found {}",
                    other.map_or("end of input".to_string(), |t| format!("'{t}'"))
                )))
            }
        }
        if !cur.eat_punct('.') {
            break;
        }
    }
    BoundPath::from_segments(path)
}

fn number_value(n: f64) -> Value {
    if n.fract() == 0.0 && n.abs() < 9.0e15 {
        Value::Number(serde_json::Number::from(n as i64))
    } else {
        serde_json::Number::from_f64(n)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// Evaluation context: the event under test, plus the origin event when
/// evaluating an `expect` join (`$origin`).
pub struct EvalCtx<'a> {
    pub event: &'a Envelope,
    pub origin: Option<&'a Envelope>,
    /// Optional per-event resolve cache (see [`PathCache`]). When present,
    /// slotted non-`$origin` paths resolve at most once per event no matter
    /// how many subscriptions reference them.
    pub cache: Option<&'a PathCache<'a>>,
}

impl<'a> EvalCtx<'a> {
    pub fn new(event: &'a Envelope, origin: Option<&'a Envelope>) -> Self {
        EvalCtx {
            event,
            origin,
            cache: None,
        }
    }
}

/// Per-event memo of slotted path resolutions, shared across all candidate
/// subscriptions evaluated against one event. Stack-inline: real events touch
/// a handful of distinct paths, so a small linear-scan table beats a heap
/// allocation per event; overflow simply falls back to direct resolution.
pub struct PathCache<'a> {
    entries: std::cell::RefCell<InlineCache<'a>>,
}

const PATH_CACHE_CAP: usize = 16;

struct InlineCache<'a> {
    keys: [u32; PATH_CACHE_CAP],
    vals: [Option<Option<Cow<'a, Value>>>; PATH_CACHE_CAP],
    len: usize,
}

impl<'a> PathCache<'a> {
    pub fn new() -> Self {
        PathCache {
            entries: std::cell::RefCell::new(InlineCache {
                keys: [0; PATH_CACHE_CAP],
                vals: [const { None }; PATH_CACHE_CAP],
                len: 0,
            }),
        }
    }
}

impl<'a> Default for PathCache<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn resolve_cached<'a>(
    bp: &'a BoundPath,
    ctx: &EvalCtx<'a>,
) -> Result<Option<Cow<'a, Value>>, EvalError> {
    let (Some(slot), Some(cache)) = (bp.slot, ctx.cache) else {
        return resolve(bp, ctx);
    };
    {
        let entries = cache.entries.borrow();
        for i in 0..entries.len {
            if entries.keys[i] == slot {
                return Ok(entries.vals[i].as_ref().unwrap().clone());
            }
        }
    }
    let resolved = resolve(bp, ctx)?;
    let mut entries = cache.entries.borrow_mut();
    if entries.len < PATH_CACHE_CAP {
        let i = entries.len;
        entries.keys[i] = slot;
        entries.vals[i] = Some(resolved.clone());
        entries.len += 1;
    }
    Ok(resolved)
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum EvalError {
    #[error("type mismatch: {0}")]
    TypeMismatch(String),
    #[error("$origin is not bound in this context")]
    NoOrigin,
    #[error("expression did not evaluate to a boolean")]
    NotBool,
}

/// Resolve a bound path. `None` means "absent" (missing payload field or
/// unset correlation) — distinct from JSON null only for `has()`.
/// Payload paths borrow from the envelope; scalar roots clone small strings.
#[inline]
fn resolve<'a>(path: &BoundPath, ctx: &EvalCtx<'a>) -> Result<Option<Cow<'a, Value>>, EvalError> {
    let env = if path.origin {
        ctx.origin.ok_or(EvalError::NoOrigin)?
    } else {
        ctx.event
    };
    let scalar = |s: &str| Some(Cow::Owned(Value::String(s.to_string())));
    let field: &'a str = match path.field {
        Field::Type => &env.event_type,
        Field::Subject => &env.subject,
        Field::Source => &env.source,
        Field::Node => &env.node,
        Field::Ts => &env.ts,
        Field::Correlation => match &env.correlation {
            Some(c) => c,
            None => return Ok(None),
        },
        Field::Payload => {
            let mut cur: &'a Value = &env.payload;
            for seg in &path.segs {
                match cur {
                    Value::Object(map) => match map.get(seg.as_str()) {
                        Some(v) => cur = v,
                        None => return Ok(None),
                    },
                    Value::Array(arr) => match seg.parse::<usize>() {
                        Ok(i) if i < arr.len() => cur = &arr[i],
                        _ => return Ok(None),
                    },
                    _ => return Ok(None),
                }
            }
            return Ok(Some(Cow::Borrowed(cur)));
        }
    };
    // Scalar envelope fields have no sub-paths; `ts.year` is simply absent.
    Ok(if path.segs.is_empty() {
        scalar(field)
    } else {
        None
    })
}

const NULL: Value = Value::Null;

fn owned<'a>(v: Value) -> Cow<'a, Value> {
    Cow::Owned(v)
}

fn eval_value<'a>(expr: &'a Expr, ctx: &EvalCtx<'a>) -> Result<Cow<'a, Value>, EvalError> {
    match expr {
        Expr::Lit(v) => Ok(Cow::Borrowed(v)),
        Expr::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for e in items {
                out.push(eval_value(e, ctx)?.into_owned());
            }
            Ok(owned(Value::Array(out)))
        }
        Expr::Path(p) => Ok(resolve_cached(p, ctx)?.unwrap_or(Cow::Borrowed(&NULL))),
        Expr::Not(e) => match eval_value(e, ctx)?.as_ref() {
            Value::Bool(b) => Ok(owned(Value::Bool(!b))),
            other => Err(EvalError::TypeMismatch(format!(
                "'!' requires a boolean, got {}",
                type_name(other)
            ))),
        },
        Expr::And(a, b) => {
            if !as_bool(eval_value(a, ctx)?.as_ref(), "&&")? {
                return Ok(owned(Value::Bool(false)));
            }
            Ok(owned(Value::Bool(as_bool(
                eval_value(b, ctx)?.as_ref(),
                "&&",
            )?)))
        }
        Expr::Or(a, b) => {
            if as_bool(eval_value(a, ctx)?.as_ref(), "||")? {
                return Ok(owned(Value::Bool(true)));
            }
            Ok(owned(Value::Bool(as_bool(
                eval_value(b, ctx)?.as_ref(),
                "||",
            )?)))
        }
        Expr::Cmp(op, a, b) => {
            let (av, bv) = (eval_value(a, ctx)?, eval_value(b, ctx)?);
            Ok(owned(Value::Bool(compare(*op, av.as_ref(), bv.as_ref())?)))
        }
        Expr::In(a, b) => {
            let av = eval_value(a, ctx)?;
            // Membership in a list literal avoids materializing the list.
            if let Expr::List(items) = b.as_ref() {
                for item in items {
                    if values_eq(eval_value(item, ctx)?.as_ref(), av.as_ref()) {
                        return Ok(owned(Value::Bool(true)));
                    }
                }
                return Ok(owned(Value::Bool(false)));
            }
            let bv = eval_value(b, ctx)?;
            match bv.as_ref() {
                Value::Array(items) => Ok(owned(Value::Bool(
                    items.iter().any(|i| values_eq(i, av.as_ref())),
                ))),
                Value::Object(map) => match av.as_ref() {
                    Value::String(k) => Ok(owned(Value::Bool(map.contains_key(k)))),
                    other => Err(EvalError::TypeMismatch(format!(
                        "'in' over a map requires a string key, got {}",
                        type_name(other)
                    ))),
                },
                other => Err(EvalError::TypeMismatch(format!(
                    "'in' requires a list or map on the right, got {}",
                    type_name(other)
                ))),
            }
        }
        Expr::Matches(a, re) => {
            let av = eval_value(a, ctx)?;
            let s = match av.as_ref() {
                Value::String(s) => s,
                other => {
                    return Err(EvalError::TypeMismatch(format!(
                        "'matches' requires a string, got {}",
                        type_name(other)
                    )))
                }
            };
            Ok(owned(Value::Bool(re.regex().is_match(s))))
        }
        Expr::Has(p) => Ok(owned(Value::Bool(resolve_cached(p, ctx)?.is_some()))),
        Expr::Size(e) => {
            let v = eval_value(e, ctx)?;
            let n = match v.as_ref() {
                Value::String(s) => s.chars().count(),
                Value::Array(a) => a.len(),
                Value::Object(m) => m.len(),
                other => {
                    return Err(EvalError::TypeMismatch(format!(
                        "size() requires a string, list or map, got {}",
                        type_name(other)
                    )))
                }
            };
            Ok(owned(Value::Number(serde_json::Number::from(n))))
        }
    }
}

/// Evaluate an expression that must produce a boolean (a `where` clause).
pub fn eval_bool<'a>(expr: &'a Expr, ctx: &EvalCtx<'a>) -> Result<bool, EvalError> {
    match eval_value(expr, ctx)?.as_ref() {
        Value::Bool(b) => Ok(*b),
        _ => Err(EvalError::NotBool),
    }
}

fn as_bool(v: &Value, op: &str) -> Result<bool, EvalError> {
    match v {
        Value::Bool(b) => Ok(*b),
        other => Err(EvalError::TypeMismatch(format!(
            "'{op}' requires booleans, got {}",
            type_name(other)
        ))),
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "list",
        Value::Object(_) => "map",
    }
}

#[inline]
fn values_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64()) {
            (Some(i), Some(j)) => i == j,
            _ => x.as_f64() == y.as_f64(),
        },
        _ => a == b,
    }
}

fn compare(op: CmpOp, a: &Value, b: &Value) -> Result<bool, EvalError> {
    match op {
        CmpOp::Eq => Ok(values_eq(a, b)),
        CmpOp::Ne => Ok(!values_eq(a, b)),
        _ => {
            let ord = match (a, b) {
                (Value::Number(x), Value::Number(y)) => x
                    .as_f64()
                    .unwrap_or(f64::NAN)
                    .partial_cmp(&y.as_f64().unwrap_or(f64::NAN)),
                (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
                _ => None,
            };
            let ord = ord.ok_or_else(|| {
                EvalError::TypeMismatch(format!(
                    "cannot order {} and {}",
                    type_name(a),
                    type_name(b)
                ))
            })?;
            Ok(match op {
                CmpOp::Lt => ord == std::cmp::Ordering::Less,
                CmpOp::Le => ord != std::cmp::Ordering::Greater,
                CmpOp::Gt => ord == std::cmp::Ordering::Greater,
                CmpOp::Ge => ord != std::cmp::Ordering::Less,
                _ => unreachable!(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env(payload: Value) -> Envelope {
        Envelope {
            id: "PH.test".into(),
            ts: "2026-08-09T12:00:00Z".into(),
            node: "trmd-mbp".into(),
            source: "tap.test".into(),
            event_type: "hive.seal".into(),
            subject: "hive.seal".into(),
            correlation: Some("HE.a3f".into()),
            payload,
            ttl_class: None,
            hops: None,
        }
    }

    fn eval_str(expr: &str, payload: Value) -> Result<bool, EvalError> {
        let e = Expr::parse_str(expr).expect("parse");
        let envelope = env(payload);
        eval_bool(&e, &EvalCtx::new(&envelope, None))
    }

    #[test]
    fn basic_comparisons() {
        assert!(eval_str(
            r#"payload.status == "blocked""#,
            json!({"status": "blocked"})
        )
        .unwrap());
        assert!(!eval_str(r#"payload.status == "blocked""#, json!({"status": "done"})).unwrap());
        assert!(eval_str("payload.count > 3", json!({"count": 5})).unwrap());
        assert!(eval_str("payload.count >= 5", json!({"count": 5})).unwrap());
        assert!(eval_str("payload.count != 4", json!({"count": 5})).unwrap());
        assert!(eval_str("payload.ratio < 0.5", json!({"ratio": 0.25})).unwrap());
    }

    #[test]
    fn boolean_logic_and_grouping() {
        let p = json!({"a": 1, "b": "x"});
        assert!(eval_str(r#"payload.a == 1 && payload.b == "x""#, p.clone()).unwrap());
        assert!(eval_str(r#"payload.a == 2 || payload.b == "x""#, p.clone()).unwrap());
        assert!(eval_str(
            r#"!(payload.a == 2) && (payload.b == "x" || payload.a == 3)"#,
            p
        )
        .unwrap());
    }

    #[test]
    fn membership_and_regex() {
        let p = json!({"labels": ["bug", "urgent"], "branch": "feature/pher-1"});
        assert!(eval_str(r#""bug" in payload.labels"#, p.clone()).unwrap());
        assert!(!eval_str(r#""docs" in payload.labels"#, p.clone()).unwrap());
        assert!(eval_str(r#"payload.branch matches "^feature/""#, p.clone()).unwrap());
        assert!(!eval_str(r#"payload.branch matches "^fix/""#, p).unwrap());
    }

    #[test]
    fn has_and_size_and_absence() {
        let p = json!({"pr": {"labels": ["a", "b"]}});
        assert!(eval_str("has(payload.pr.labels)", p.clone()).unwrap());
        assert!(!eval_str("has(payload.pr.reviewer)", p.clone()).unwrap());
        assert!(eval_str("size(payload.pr.labels) == 2", p.clone()).unwrap());
        // Absent path behaves as null for equality.
        assert!(eval_str("payload.missing == null", p).unwrap());
    }

    #[test]
    fn envelope_bindings() {
        assert!(eval_str(r#"type == "hive.seal""#, json!({})).unwrap());
        assert!(eval_str(r#"node == "trmd-mbp" && source == "tap.test""#, json!({})).unwrap());
        assert!(eval_str(r#"correlation == "HE.a3f""#, json!({})).unwrap());
        assert!(eval_str(r#"ts > "2026-01-01T00:00:00Z""#, json!({})).unwrap());
    }

    #[test]
    fn origin_binding() {
        let e = Expr::parse_str("correlation == $origin.correlation").unwrap();
        let event = env(json!({}));
        let origin = env(json!({}));
        assert!(eval_bool(&e, &EvalCtx::new(&event, Some(&origin))).unwrap());
        assert_eq!(
            eval_bool(&e, &EvalCtx::new(&event, None)),
            Err(EvalError::NoOrigin)
        );
    }

    #[test]
    fn type_errors_are_reported() {
        assert!(eval_str("payload.count > 3", json!({"count": "five"})).is_err());
        assert!(eval_str(r#"payload.status"#, json!({"status": "x"})).is_err());
        // NotBool
    }

    #[test]
    fn unknown_root_rejected_at_parse() {
        assert!(Expr::parse_str("data.x == 1").is_err());
        assert!(Expr::parse_str("payload.x == 1").is_ok());
    }

    #[test]
    fn canonical_printing_round_trip() {
        let cases = [
            r#"payload.conclusion == "failure" && payload.branch == "main""#,
            r#"(payload.a == 1 || payload.b == 2) && !has(payload.skip)"#,
            r#""bug" in payload.labels"#,
            r#"payload.branch matches "^feature/""#,
            r#"size(payload.items) >= 3"#,
            r#"payload.env in ["prod", "staging"]"#,
        ];
        for c in cases {
            let e = Expr::parse_str(c).unwrap();
            let printed = e.canon();
            let re = Expr::parse_str(&printed).unwrap();
            assert_eq!(e, re, "round-trip changed AST for {c}");
            assert_eq!(printed, re.canon(), "printing not stable for {c}");
        }
    }
}
