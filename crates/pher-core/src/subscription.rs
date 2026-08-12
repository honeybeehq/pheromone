use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::duration::Dur;
use crate::expr::Expr;
use crate::lexer::{lex, quote, Cursor, Tok};
use crate::subject::SubjectPattern;
use crate::ParseError;

pub const DEFAULT_MEANING_THRESHOLD: f64 = 0.75;
pub const DEFAULT_NOVEL_WINDOW: &str = "7d";

/// A parsed subscription — the canonical structured form of the language.
#[derive(Debug, Clone, PartialEq)]
pub struct Subscription {
    pub on: Vec<SubjectPattern>,
    pub from: Option<SubjectPattern>,
    pub where_expr: Option<Expr>,
    pub meaning: Option<Meaning>,
    pub judge: Option<Judge>,
    pub expect: Option<Expect>,
    pub then: Action,
    pub lifetime: Lifetime,
    pub delivery: Delivery,
    pub replay: Option<Replay>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Meaning {
    pub kind: MeaningKind,
    pub threshold: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MeaningKind {
    /// `meaning "..."` / `meaning any of ["...", ...]` — max over descriptors.
    Descriptors(Vec<String>),
    /// `meaning novel [> t] [over 7d]` — far from every event in the window.
    Novel { over: Dur },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Judge {
    pub question: String,
    pub budget_count: u64,
    /// One of `min`, `hour`, `day`, `week`.
    pub budget_period: String,
    pub sample: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Expect {
    pub subject: SubjectPattern,
    pub where_expr: Option<Expr>,
    pub within: Dur,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Sink {
    Buz,
    Hive,
    Http,
    Cmd,
    Hermes,
    Pol,
    Emit,
    /// Deliver to the live client connection that registered the
    /// subscription (SDK/`pher listen`). Connection-scoped by construction:
    /// the subscription dies when the listener disconnects.
    Stream,
}

impl Sink {
    pub fn parse(s: &str) -> Option<Sink> {
        Some(match s {
            "buz" => Sink::Buz,
            "hive" => Sink::Hive,
            "http" => Sink::Http,
            "cmd" => Sink::Cmd,
            "hermes" => Sink::Hermes,
            "pol" => Sink::Pol,
            "emit" => Sink::Emit,
            "stream" => Sink::Stream,
            _ => return None,
        })
    }

    pub fn name(&self) -> &'static str {
        match self {
            Sink::Buz => "buz",
            Sink::Hive => "hive",
            Sink::Http => "http",
            Sink::Cmd => "cmd",
            Sink::Hermes => "hermes",
            Sink::Pol => "pol",
            Sink::Emit => "emit",
            Sink::Stream => "stream",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Action {
    pub sink: Sink,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Lifetime {
    Durable,
    Ttl { ttl: Dur },
    Lease { lessee: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum Delivery {
    Immediate,
    Debounce { window: Dur },
    Batch { window: Dur },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Replay {
    pub lookback: Dur,
    pub reevaluate: bool,
}

// ---------------------------------------------------------------------------
// String form → Subscription
// ---------------------------------------------------------------------------

impl Subscription {
    /// Parse the canonical string form. A leading `when` is optional (the CLI
    /// verb `pher when '<sub>'` supplies it).
    pub fn parse(input: &str) -> Result<Subscription, ParseError> {
        let input = input.trim();
        let (head, tail) = split_at_then(input)?;
        let head = head.trim();
        let head = head.strip_prefix("when ").unwrap_or(head).trim();
        if head.is_empty() {
            return Err(ParseError::new("subscription has no clauses before 'then'"));
        }

        let toks = lex(head)?;
        let mut cur = Cursor::new(&toks);

        // Clauses in canonical (cost-cascade) order. `on` is mandatory.
        cur.expect_keyword("on").map_err(|_| {
            ParseError::new(
                "every subscription starts with 'on <subject-pattern>' (tier 1 is mandatory)",
            )
        })?;
        let mut on = vec![SubjectPattern::parse_tokens(&mut cur)?];
        while cur.eat_punct(',') {
            on.push(SubjectPattern::parse_tokens(&mut cur)?);
        }

        let from = if cur.eat_keyword("from") {
            Some(SubjectPattern::parse_tokens(&mut cur)?)
        } else {
            None
        };

        let where_expr = if cur.eat_keyword("where") {
            Some(Expr::parse(&mut cur)?)
        } else {
            None
        };

        let meaning = if cur.eat_keyword("meaning") {
            Some(parse_meaning(&mut cur)?)
        } else {
            None
        };

        let judge = if cur.eat_keyword("judge") {
            Some(parse_judge(&mut cur)?)
        } else {
            None
        };

        let expect = if cur.eat_keyword("expect") {
            Some(parse_expect(&mut cur)?)
        } else {
            None
        };

        if !cur.at_end() {
            return Err(ParseError::new(format!(
                "unexpected {} — clauses must appear in order: on, from, where, meaning, judge, expect",
                cur.describe_next()
            )));
        }

        // Tail: action words, then trailing options.
        let words = split_words(tail)?;
        if words.is_empty() {
            return Err(ParseError::new("missing action after 'then'"));
        }
        let sink = Sink::parse(&words[0]).ok_or_else(|| {
            ParseError::new(format!(
                "unknown sink '{}' (expected buz, hive, http, cmd, hermes, pol, emit)",
                words[0]
            ))
        })?;
        let (args, opts) = split_action_options(&words[1..])?;
        let mut sub = Subscription {
            on,
            from,
            where_expr,
            meaning,
            judge,
            expect,
            then: Action {
                sink,
                args: args.to_vec(),
            },
            lifetime: Lifetime::Durable,
            delivery: Delivery::Immediate,
            replay: None,
            limit: None,
        };
        apply_options(&mut sub, &opts)?;
        Ok(sub)
    }

    /// Canonical string form. `parse(canon(s)) == s` and `canon` is idempotent.
    pub fn canon(&self) -> String {
        let mut parts: Vec<String> = vec!["when".to_string()];
        let subjects: Vec<String> = self.on.iter().map(|p| p.to_string()).collect();
        parts.push(format!("on {}", subjects.join(", ")));
        if let Some(from) = &self.from {
            parts.push(format!("from {from}"));
        }
        if let Some(w) = &self.where_expr {
            parts.push(format!("where {}", w.canon()));
        }
        if let Some(m) = &self.meaning {
            parts.push(format!("meaning {}", meaning_canon(m)));
        }
        if let Some(j) = &self.judge {
            let mut s = format!(
                "judge {} budget {}/{}",
                quote(&j.question),
                j.budget_count,
                j.budget_period
            );
            if let Some(sample) = j.sample {
                s.push_str(&format!(" sample {sample}"));
            }
            parts.push(s);
        }
        if let Some(e) = &self.expect {
            let mut s = format!("expect {}", e.subject);
            if let Some(w) = &e.where_expr {
                s.push_str(&format!(" where {}", w.canon()));
            }
            s.push_str(&format!(" within {} else", e.within));
            parts.push(s);
        }
        let mut action = vec!["then".to_string(), self.then.sink.name().to_string()];
        action.extend(self.then.args.iter().map(|a| quote_word(a)));
        parts.push(action.join(" "));

        match &self.lifetime {
            Lifetime::Durable => {}
            Lifetime::Ttl { ttl } => parts.push(format!("for {ttl}")),
            Lifetime::Lease { lessee } => parts.push(format!("while {lessee} alive")),
        }
        match &self.delivery {
            Delivery::Immediate => {}
            Delivery::Debounce { window } => parts.push(format!("every {window}")),
            Delivery::Batch { window } => parts.push(format!("batch {window}")),
        }
        if let Some(r) = &self.replay {
            parts.push(format!(
                "since {}{}",
                r.lookback,
                if r.reevaluate { " reevaluate" } else { "" }
            ));
        }
        if let Some(n) = self.limit {
            parts.push(format!("limit {n}"));
        }
        parts.join(" ")
    }

    /// Canonical JSON form (the API/SDK/storage representation).
    pub fn to_json(&self) -> Value {
        let meaning = self.meaning.as_ref().map(|m| match &m.kind {
            MeaningKind::Descriptors(d) => serde_json::json!({
                "descriptors": d,
                "threshold": m.threshold,
            }),
            MeaningKind::Novel { over } => serde_json::json!({
                "novel": true,
                "threshold": m.threshold,
                "over": over.text(),
            }),
        });
        let judge = self.judge.as_ref().map(|j| {
            serde_json::json!({
                "question": j.question,
                "budget": { "count": j.budget_count, "period": j.budget_period },
                "sample": j.sample,
            })
        });
        let expect = self.expect.as_ref().map(|e| {
            serde_json::json!({
                "on": e.subject.to_string(),
                "where": e.where_expr.as_ref().map(|w| w.canon()),
                "within": e.within.text(),
            })
        });
        serde_json::json!({
            "on": self.on.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
            "from": self.from.as_ref().map(|p| p.to_string()),
            "where": self.where_expr.as_ref().map(|w| w.canon()),
            "meaning": meaning,
            "judge": judge,
            "expect": expect,
            "then": { "sink": self.then.sink.name(), "args": self.then.args },
            "lifetime": self.lifetime,
            "delivery": self.delivery,
            "replay": self.replay.as_ref().map(|r| serde_json::json!({
                "lookback": r.lookback.text(),
                "reevaluate": r.reevaluate,
            })),
            "limit": self.limit,
        })
    }

    pub fn from_json(v: &Value) -> Result<Subscription, ParseError> {
        let obj = v
            .as_object()
            .ok_or_else(|| ParseError::new("subscription JSON must be an object"))?;
        let on = obj
            .get("on")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ParseError::new("'on' must be a list of subject patterns"))?
            .iter()
            .map(|s| {
                s.as_str()
                    .ok_or_else(|| ParseError::new("'on' entries must be strings"))
                    .and_then(SubjectPattern::parse)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if on.is_empty() {
            return Err(ParseError::new("'on' must not be empty"));
        }
        let from = match obj.get("from") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(SubjectPattern::parse(s)?),
            Some(_) => return Err(ParseError::new("'from' must be a string or null")),
        };
        let where_expr = match obj.get("where") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(Expr::parse_str(s)?),
            Some(_) => return Err(ParseError::new("'where' must be a string or null")),
        };
        let meaning = match obj.get("meaning") {
            None | Some(Value::Null) => None,
            Some(m) => Some(meaning_from_json(m)?),
        };
        let judge = match obj.get("judge") {
            None | Some(Value::Null) => None,
            Some(j) => {
                let question = j
                    .get("question")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ParseError::new("judge.question must be a string"))?
                    .to_string();
                let budget = j
                    .get("budget")
                    .ok_or_else(|| ParseError::new("judge.budget is mandatory"))?;
                let count = budget
                    .get("count")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| ParseError::new("judge.budget.count must be an integer"))?;
                let period = budget
                    .get("period")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ParseError::new("judge.budget.period must be a string"))?;
                validate_period(period)?;
                Some(Judge {
                    question,
                    budget_count: count,
                    budget_period: period.to_string(),
                    sample: j.get("sample").and_then(|v| v.as_f64()),
                })
            }
        };
        let expect = match obj.get("expect") {
            None | Some(Value::Null) => None,
            Some(e) => {
                let subject = e
                    .get("on")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ParseError::new("expect.on must be a string"))?;
                let where_expr = match e.get("where") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(s)) => Some(Expr::parse_str(s)?),
                    Some(_) => return Err(ParseError::new("expect.where must be a string")),
                };
                let within = e
                    .get("within")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| ParseError::new("expect.within must be a duration string"))?;
                Some(Expect {
                    subject: SubjectPattern::parse(subject)?,
                    where_expr,
                    within: Dur::parse(within)?,
                })
            }
        };
        let then = obj
            .get("then")
            .ok_or_else(|| ParseError::new("'then' is mandatory"))?;
        let sink = then
            .get("sink")
            .and_then(|v| v.as_str())
            .and_then(Sink::parse)
            .ok_or_else(|| {
                ParseError::new("then.sink must be one of buz/hive/http/cmd/hermes/pol/emit")
            })?;
        let args = then
            .get("args")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .map(|x| {
                        x.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| ParseError::new("then.args must be strings"))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        let lifetime = match obj.get("lifetime") {
            None | Some(Value::Null) => Lifetime::Durable,
            Some(l) => serde_json::from_value(l.clone())
                .map_err(|e| ParseError::new(format!("invalid lifetime: {e}")))?,
        };
        let delivery = match obj.get("delivery") {
            None | Some(Value::Null) => Delivery::Immediate,
            Some(d) => serde_json::from_value(d.clone())
                .map_err(|e| ParseError::new(format!("invalid delivery: {e}")))?,
        };
        let replay = match obj.get("replay") {
            None | Some(Value::Null) => None,
            Some(r) => Some(Replay {
                lookback: Dur::parse(
                    r.get("lookback")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| ParseError::new("replay.lookback must be a duration"))?,
                )?,
                reevaluate: r
                    .get("reevaluate")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            }),
        };
        let limit = match obj.get("limit") {
            None | Some(Value::Null) => None,
            Some(v) => Some(
                v.as_u64()
                    .ok_or_else(|| ParseError::new("'limit' must be a positive integer"))?,
            ),
        };
        Ok(Subscription {
            on,
            from,
            where_expr,
            meaning,
            judge,
            expect,
            then: Action { sink, args },
            lifetime,
            delivery,
            replay,
            limit,
        })
    }
}

fn meaning_canon(m: &Meaning) -> String {
    let threshold = if (m.threshold - DEFAULT_MEANING_THRESHOLD).abs() > f64::EPSILON {
        format!(" > {}", m.threshold)
    } else {
        String::new()
    };
    match &m.kind {
        MeaningKind::Descriptors(d) if d.len() == 1 => format!("{}{threshold}", quote(&d[0])),
        MeaningKind::Descriptors(d) => {
            let items: Vec<String> = d.iter().map(|s| quote(s)).collect();
            format!("any of [{}]{threshold}", items.join(", "))
        }
        MeaningKind::Novel { over } => {
            let over_s = if over.text() != DEFAULT_NOVEL_WINDOW {
                format!(" over {over}")
            } else {
                String::new()
            };
            format!("novel{threshold}{over_s}")
        }
    }
}

fn meaning_from_json(m: &Value) -> Result<Meaning, ParseError> {
    let threshold = m
        .get("threshold")
        .and_then(|v| v.as_f64())
        .unwrap_or(DEFAULT_MEANING_THRESHOLD);
    if m.get("novel").and_then(|v| v.as_bool()) == Some(true) {
        let over = m
            .get("over")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_NOVEL_WINDOW);
        return Ok(Meaning {
            kind: MeaningKind::Novel {
                over: Dur::parse(over)?,
            },
            threshold,
        });
    }
    let descriptors = m
        .get("descriptors")
        .and_then(|v| v.as_array())
        .ok_or_else(|| ParseError::new("meaning.descriptors must be a list of strings"))?
        .iter()
        .map(|d| {
            d.as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| ParseError::new("meaning.descriptors must be strings"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if descriptors.is_empty() {
        return Err(ParseError::new("meaning.descriptors must not be empty"));
    }
    Ok(Meaning {
        kind: MeaningKind::Descriptors(descriptors),
        threshold,
    })
}

fn parse_meaning(cur: &mut Cursor) -> Result<Meaning, ParseError> {
    // meaning "desc" [> t] | meaning any of [ ... ] [> t] | meaning novel [> t] [over dur]
    if cur.eat_keyword("any") {
        cur.expect_keyword("of")?;
        cur.expect_punct('[')?;
        let mut descriptors = Vec::new();
        loop {
            match cur.next() {
                Some(Tok::Str(s)) => descriptors.push(s.clone()),
                other => {
                    return Err(ParseError::new(format!(
                        "'meaning any of' expects string descriptors, found {}",
                        other.map_or("end of input".to_string(), |t| format!("'{t}'"))
                    )))
                }
            }
            if cur.eat_punct(']') {
                break;
            }
            cur.expect_punct(',')?;
        }
        let threshold = parse_threshold(cur)?.unwrap_or(DEFAULT_MEANING_THRESHOLD);
        return Ok(Meaning {
            kind: MeaningKind::Descriptors(descriptors),
            threshold,
        });
    }
    if cur.eat_keyword("novel") {
        let threshold = parse_threshold(cur)?.unwrap_or(DEFAULT_MEANING_THRESHOLD);
        let over = if cur.eat_keyword("over") {
            parse_duration(cur)?
        } else {
            Dur::parse(DEFAULT_NOVEL_WINDOW).unwrap()
        };
        return Ok(Meaning {
            kind: MeaningKind::Novel { over },
            threshold,
        });
    }
    match cur.next() {
        Some(Tok::Str(s)) => {
            let threshold = parse_threshold(cur)?.unwrap_or(DEFAULT_MEANING_THRESHOLD);
            Ok(Meaning {
                kind: MeaningKind::Descriptors(vec![s.clone()]),
                threshold,
            })
        }
        other => Err(ParseError::new(format!(
            "'meaning' expects a string descriptor, 'any of [...]', or 'novel'; found {}",
            other.map_or("end of input".to_string(), |t| format!("'{t}'"))
        ))),
    }
}

fn parse_threshold(cur: &mut Cursor) -> Result<Option<f64>, ParseError> {
    if cur.eat_op(">") || cur.eat_op(">=") {
        match cur.next() {
            Some(Tok::Num(n)) if (0.0..=1.0).contains(n) => Ok(Some(*n)),
            Some(Tok::Num(n)) => Err(ParseError::new(format!(
                "similarity threshold must be between 0 and 1, got {n}"
            ))),
            other => Err(ParseError::new(format!(
                "expected threshold number, found {}",
                other.map_or("end of input".to_string(), |t| format!("'{t}'"))
            ))),
        }
    } else {
        Ok(None)
    }
}

fn parse_judge(cur: &mut Cursor) -> Result<Judge, ParseError> {
    let question = match cur.next() {
        Some(Tok::Str(s)) => s.clone(),
        other => {
            return Err(ParseError::new(format!(
                "'judge' expects a quoted yes/no question, found {}",
                other.map_or("end of input".to_string(), |t| format!("'{t}'"))
            )))
        }
    };
    cur.expect_keyword("budget").map_err(|_| {
        ParseError::new("'judge' requires a budget: judge \"...\" budget <n>/<period>")
    })?;
    let count = match cur.next() {
        Some(Tok::Num(n)) if n.fract() == 0.0 && *n > 0.0 => *n as u64,
        other => {
            return Err(ParseError::new(format!(
                "budget expects a positive integer, found {}",
                other.map_or("end of input".to_string(), |t| format!("'{t}'"))
            )))
        }
    };
    cur.expect_punct('/')?;
    let period = match cur.next() {
        Some(Tok::Ident(p)) => p.clone(),
        other => {
            return Err(ParseError::new(format!(
                "budget expects a period (min/hour/day/week), found {}",
                other.map_or("end of input".to_string(), |t| format!("'{t}'"))
            )))
        }
    };
    validate_period(&period)?;
    let sample = if cur.eat_keyword("sample") {
        match cur.next() {
            Some(Tok::Num(n)) if (0.0..=1.0).contains(n) => Some(*n),
            other => {
                return Err(ParseError::new(format!(
                    "'sample' expects a number in [0,1], found {}",
                    other.map_or("end of input".to_string(), |t| format!("'{t}'"))
                )))
            }
        }
    } else {
        None
    };
    Ok(Judge {
        question,
        budget_count: count,
        budget_period: period,
        sample,
    })
}

fn validate_period(p: &str) -> Result<(), ParseError> {
    match p {
        "min" | "hour" | "day" | "week" => Ok(()),
        other => Err(ParseError::new(format!(
            "unknown budget period '{other}' (use min, hour, day, week)"
        ))),
    }
}

fn parse_expect(cur: &mut Cursor) -> Result<Expect, ParseError> {
    let subject = SubjectPattern::parse_tokens(cur)?;
    let where_expr = if cur.eat_keyword("where") {
        Some(Expr::parse(cur)?)
    } else {
        None
    };
    cur.expect_keyword("within")?;
    let within = parse_duration(cur)?;
    cur.expect_keyword("else")?;
    Ok(Expect {
        subject,
        where_expr,
        within,
    })
}

/// Durations lex as Num + Ident (`2h` → 2, "h"); `within 2 h` is equivalent.
fn parse_duration(cur: &mut Cursor) -> Result<Dur, ParseError> {
    match (cur.next(), cur.next()) {
        (Some(Tok::Num(n)), Some(Tok::Ident(unit))) if n.fract() == 0.0 && *n >= 0.0 => {
            Dur::from_parts(*n as u64, unit)
        }
        (a, _) => Err(ParseError::new(format!(
            "expected a duration like 30m/2h/7d, found {}",
            a.map_or("end of input".to_string(), |t| format!("'{t}'"))
        ))),
    }
}

// ---------------------------------------------------------------------------
// Tail helpers: `then <action words> <options>`
// ---------------------------------------------------------------------------

/// Find the top-level ` then ` keyword (outside quotes) and split there.
fn split_at_then(input: &str) -> Result<(&str, &str), ParseError> {
    let bytes = input.as_bytes();
    let mut in_quote: Option<u8> = None;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match in_quote {
            Some(q) => {
                if b == b'\\' {
                    i += 1;
                } else if b == q {
                    in_quote = None;
                }
            }
            None => {
                if b == b'"' || b == b'\'' {
                    in_quote = Some(b);
                } else if input[i..].starts_with("then")
                    && (i == 0 || bytes[i - 1].is_ascii_whitespace())
                    && input[i + 4..]
                        .chars()
                        .next()
                        .is_none_or(|c| c.is_whitespace())
                {
                    return Ok((&input[..i], &input[i + 4..]));
                }
            }
        }
        i += 1;
    }
    Err(ParseError::new(
        "missing 'then <action>' — every subscription needs an action",
    ))
}

/// Shell-like word splitting for the action tail; quotes group words.
fn split_words(input: &str) -> Result<Vec<String>, ParseError> {
    let chars: Vec<char> = input.chars().collect();
    let mut words = Vec::new();
    let mut cur = String::new();
    let mut i = 0;
    let mut in_word = false;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' || c == '\'' {
            let (s, next) = crate::lexer::lex_string(&chars, i)?;
            cur.push_str(&s);
            in_word = true;
            i = next;
        } else if c.is_whitespace() {
            if in_word {
                words.push(std::mem::take(&mut cur));
                in_word = false;
            }
            i += 1;
        } else {
            cur.push(c);
            in_word = true;
            i += 1;
        }
    }
    if in_word {
        words.push(cur);
    }
    Ok(words)
}

fn quote_word(w: &str) -> String {
    if w.is_empty()
        || w.chars()
            .any(|c| c.is_whitespace() || c == '"' || c == '\'')
    {
        quote(w)
    } else {
        w.to_string()
    }
}

#[derive(Debug)]
enum Opt {
    For(Dur),
    While(String),
    Every(Dur),
    Batch(Dur),
    Since(Dur, bool),
    Limit(u64),
}

/// Split `words` into (action args, trailing options): the longest suffix that
/// parses entirely as options is treated as options. Quote args that collide
/// with option keywords.
fn split_action_options(words: &[String]) -> Result<(&[String], Vec<Opt>), ParseError> {
    for i in 0..=words.len() {
        if let Some(opts) = try_parse_options(&words[i..]) {
            return Ok((&words[..i], opts));
        }
    }
    unreachable!("empty suffix always parses as zero options")
}

fn try_parse_options(words: &[String]) -> Option<Vec<Opt>> {
    let mut opts = Vec::new();
    let mut i = 0;
    while i < words.len() {
        match words[i].as_str() {
            "for" => {
                let d = Dur::parse(words.get(i + 1)?).ok()?;
                opts.push(Opt::For(d));
                i += 2;
            }
            "while" => {
                let lessee = words.get(i + 1)?.clone();
                if words.get(i + 2).map(|s| s.as_str()) != Some("alive") {
                    return None;
                }
                opts.push(Opt::While(lessee));
                i += 3;
            }
            "every" => {
                let d = Dur::parse(words.get(i + 1)?).ok()?;
                opts.push(Opt::Every(d));
                i += 2;
            }
            "batch" => {
                let d = Dur::parse(words.get(i + 1)?).ok()?;
                opts.push(Opt::Batch(d));
                i += 2;
            }
            "since" => {
                let d = Dur::parse(words.get(i + 1)?).ok()?;
                let reeval = words.get(i + 2).map(|s| s.as_str()) == Some("reevaluate");
                opts.push(Opt::Since(d, reeval));
                i += if reeval { 3 } else { 2 };
            }
            "limit" => {
                let n: u64 = words.get(i + 1)?.parse().ok()?;
                opts.push(Opt::Limit(n));
                i += 2;
            }
            _ => return None,
        }
    }
    Some(opts)
}

fn apply_options(sub: &mut Subscription, opts: &[Opt]) -> Result<(), ParseError> {
    for opt in opts {
        match opt {
            Opt::For(d) => set_lifetime(sub, Lifetime::Ttl { ttl: d.clone() })?,
            Opt::While(lessee) => set_lifetime(
                sub,
                Lifetime::Lease {
                    lessee: lessee.clone(),
                },
            )?,
            Opt::Every(d) => set_delivery(sub, Delivery::Debounce { window: d.clone() })?,
            Opt::Batch(d) => set_delivery(sub, Delivery::Batch { window: d.clone() })?,
            Opt::Since(d, reeval) => {
                if sub.replay.is_some() {
                    return Err(ParseError::new("duplicate 'since' option"));
                }
                sub.replay = Some(Replay {
                    lookback: d.clone(),
                    reevaluate: *reeval,
                });
            }
            Opt::Limit(n) => {
                if sub.limit.is_some() {
                    return Err(ParseError::new("duplicate 'limit' option"));
                }
                if *n == 0 {
                    return Err(ParseError::new("'limit' must be at least 1"));
                }
                sub.limit = Some(*n);
            }
        }
    }
    Ok(())
}

/// Merge CLI-flag options (e.g. `--while CL.6308 alive` passed as flags) into a
/// parsed subscription. Used by `pher when`.
pub fn apply_option_words(sub: &mut Subscription, words: &[String]) -> Result<(), ParseError> {
    let opts = try_parse_options(words)
        .ok_or_else(|| ParseError::new(format!("invalid option words: {}", words.join(" "))))?;
    apply_options(sub, &opts)
}

fn set_lifetime(sub: &mut Subscription, l: Lifetime) -> Result<(), ParseError> {
    if sub.lifetime != Lifetime::Durable {
        return Err(ParseError::new(
            "conflicting lifetime options ('for' and 'while' are mutually exclusive)",
        ));
    }
    sub.lifetime = l;
    Ok(())
}

fn set_delivery(sub: &mut Subscription, d: Delivery) -> Result<(), ParseError> {
    if sub.delivery != Delivery::Immediate {
        return Err(ParseError::new(
            "conflicting delivery options ('every' and 'batch' are mutually exclusive)",
        ));
    }
    sub.delivery = d;
    Ok(())
}
