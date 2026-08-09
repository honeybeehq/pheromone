use crate::ParseError;

/// Token for the subscription-language head (clauses). Actions are word-split
/// separately because their args are free-form (`./triage.sh`, `--tier`, URLs).
#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    /// Identifier / keyword / subject token. May contain `-` and `_`, and may
    /// start with `$` (`$origin`).
    Ident(String),
    Num(f64),
    Str(String),
    /// One of `( ) [ ] , . /`
    Punct(char),
    /// One of `== != <= >= < > && || ! * **`
    Op(&'static str),
}

impl std::fmt::Display for Tok {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Tok::Ident(s) => write!(f, "{s}"),
            Tok::Num(n) => write!(f, "{n}"),
            Tok::Str(s) => write!(f, "{s:?}"),
            Tok::Punct(c) => write!(f, "{c}"),
            Tok::Op(o) => write!(f, "{o}"),
        }
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == '$'
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// Lex a clause-section string into tokens.
pub fn lex(input: &str) -> Result<Vec<Tok>, ParseError> {
    let mut toks = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '(' | ')' | '[' | ']' | ',' | '.' | '/' => {
                toks.push(Tok::Punct(c));
                i += 1;
            }
            '"' | '\'' => {
                let (s, next) = lex_string(&chars, i)?;
                toks.push(Tok::Str(s));
                i = next;
            }
            '=' if chars.get(i + 1) == Some(&'=') => {
                toks.push(Tok::Op("=="));
                i += 2;
            }
            '!' if chars.get(i + 1) == Some(&'=') => {
                toks.push(Tok::Op("!="));
                i += 2;
            }
            '!' => {
                toks.push(Tok::Op("!"));
                i += 1;
            }
            '<' if chars.get(i + 1) == Some(&'=') => {
                toks.push(Tok::Op("<="));
                i += 2;
            }
            '<' => {
                toks.push(Tok::Op("<"));
                i += 1;
            }
            '>' if chars.get(i + 1) == Some(&'=') => {
                toks.push(Tok::Op(">="));
                i += 2;
            }
            '>' => {
                toks.push(Tok::Op(">"));
                i += 1;
            }
            '&' if chars.get(i + 1) == Some(&'&') => {
                toks.push(Tok::Op("&&"));
                i += 2;
            }
            '|' if chars.get(i + 1) == Some(&'|') => {
                toks.push(Tok::Op("||"));
                i += 2;
            }
            '*' if chars.get(i + 1) == Some(&'*') => {
                toks.push(Tok::Op("**"));
                i += 2;
            }
            '*' => {
                toks.push(Tok::Op("*"));
                i += 1;
            }
            '-' if chars.get(i + 1).is_some_and(|c| c.is_ascii_digit())
                && number_sign_position(&toks) =>
            {
                let (n, next) = lex_number(&chars, i + 1)?;
                toks.push(Tok::Num(-n));
                i = next;
            }
            c if c.is_ascii_digit() => {
                let (n, next) = lex_number(&chars, i)?;
                toks.push(Tok::Num(n));
                i = next;
            }
            c if is_ident_start(c) => {
                let start = i;
                i += 1;
                while i < chars.len() && is_ident_continue(chars[i]) {
                    i += 1;
                }
                toks.push(Tok::Ident(chars[start..i].iter().collect()));
            }
            other => {
                return Err(ParseError::new(format!(
                    "unexpected character '{other}' at byte {i}"
                )))
            }
        }
    }
    Ok(toks)
}

/// A leading `-` is a numeric sign only where a value is expected.
fn number_sign_position(toks: &[Tok]) -> bool {
    match toks.last() {
        None => true,
        Some(Tok::Op(_)) => true,
        Some(Tok::Punct(c)) => matches!(c, '(' | '[' | ','),
        Some(Tok::Ident(_)) | Some(Tok::Num(_)) | Some(Tok::Str(_)) => true,
    }
}

fn lex_number(chars: &[char], start: usize) -> Result<(f64, usize), ParseError> {
    let mut i = start;
    while i < chars.len() && chars[i].is_ascii_digit() {
        i += 1;
    }
    if i < chars.len() && chars[i] == '.' && chars.get(i + 1).is_some_and(|c| c.is_ascii_digit()) {
        i += 1;
        while i < chars.len() && chars[i].is_ascii_digit() {
            i += 1;
        }
    }
    let text: String = chars[start..i].iter().collect();
    let n: f64 = text
        .parse()
        .map_err(|_| ParseError::new(format!("invalid number '{text}'")))?;
    Ok((n, i))
}

/// Lex a quoted string starting at `chars[start]` (the quote char).
/// Returns the unescaped contents and the index after the closing quote.
pub fn lex_string(chars: &[char], start: usize) -> Result<(String, usize), ParseError> {
    let quote = chars[start];
    let mut out = String::new();
    let mut i = start + 1;
    while i < chars.len() {
        let c = chars[i];
        if c == quote {
            return Ok((out, i + 1));
        }
        if c == '\\' {
            let esc = chars
                .get(i + 1)
                .ok_or_else(|| ParseError::new("unterminated escape in string"))?;
            match esc {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                '\\' => out.push('\\'),
                '"' => out.push('"'),
                '\'' => out.push('\''),
                other => {
                    return Err(ParseError::new(format!("unknown escape '\\{other}'")));
                }
            }
            i += 2;
        } else {
            out.push(c);
            i += 1;
        }
    }
    Err(ParseError::new("unterminated string literal"))
}

/// Escape and double-quote a string for canonical output.
pub fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// A cursor over a token slice, shared by the expression and subscription parsers.
pub struct Cursor<'a> {
    toks: &'a [Tok],
    pub pos: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(toks: &'a [Tok]) -> Self {
        Cursor { toks, pos: 0 }
    }

    pub fn peek(&self) -> Option<&'a Tok> {
        self.toks.get(self.pos)
    }

    pub fn peek_at(&self, offset: usize) -> Option<&'a Tok> {
        self.toks.get(self.pos + offset)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<&'a Tok> {
        let t = self.toks.get(self.pos);
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    pub fn at_end(&self) -> bool {
        self.pos >= self.toks.len()
    }

    /// Consume an ident equal to `kw`; return whether it was there.
    pub fn eat_keyword(&mut self, kw: &str) -> bool {
        if let Some(Tok::Ident(s)) = self.peek() {
            if s == kw {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    pub fn expect_keyword(&mut self, kw: &str) -> Result<(), ParseError> {
        if self.eat_keyword(kw) {
            Ok(())
        } else {
            Err(ParseError::new(format!(
                "expected '{kw}', found {}",
                self.describe_next()
            )))
        }
    }

    pub fn eat_punct(&mut self, c: char) -> bool {
        if self.peek() == Some(&Tok::Punct(c)) {
            self.pos += 1;
            return true;
        }
        false
    }

    pub fn expect_punct(&mut self, c: char) -> Result<(), ParseError> {
        if self.eat_punct(c) {
            Ok(())
        } else {
            Err(ParseError::new(format!(
                "expected '{c}', found {}",
                self.describe_next()
            )))
        }
    }

    pub fn eat_op(&mut self, op: &str) -> bool {
        if let Some(Tok::Op(o)) = self.peek() {
            if *o == op {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    pub fn describe_next(&self) -> String {
        match self.peek() {
            Some(t) => format!("'{t}'"),
            None => "end of input".to_string(),
        }
    }
}
