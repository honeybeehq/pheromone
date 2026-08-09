use serde::{Deserialize, Serialize};

use crate::ParseError;

/// A duration literal like `30m`, `2h`, `7d`. Canonical text form is preserved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Dur {
    text: String,
    secs: u64,
}

impl Dur {
    pub fn parse(text: &str) -> Result<Self, ParseError> {
        let text = text.trim();
        let split = text
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(|| ParseError::new(format!("duration '{text}' is missing a unit")))?;
        let (num, unit) = text.split_at(split);
        let n: u64 = num
            .parse()
            .map_err(|_| ParseError::new(format!("invalid duration '{text}'")))?;
        let mult = match unit {
            "s" => 1,
            "m" => 60,
            "h" => 3600,
            "d" => 86400,
            "w" => 604800,
            other => {
                return Err(ParseError::new(format!(
                    "unknown duration unit '{other}' (use s, m, h, d, w)"
                )))
            }
        };
        Ok(Dur {
            text: format!("{n}{unit}"),
            secs: n
                .checked_mul(mult)
                .ok_or_else(|| ParseError::new("duration overflow"))?,
        })
    }

    pub fn from_parts(n: u64, unit: &str) -> Result<Self, ParseError> {
        Self::parse(&format!("{n}{unit}"))
    }

    pub fn secs(&self) -> u64 {
        self.secs
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

impl std::fmt::Display for Dur {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.text)
    }
}

impl TryFrom<String> for Dur {
    type Error = ParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Dur::parse(&s)
    }
}

impl From<Dur> for String {
    fn from(d: Dur) -> String {
        d.text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units() {
        assert_eq!(Dur::parse("30m").unwrap().secs(), 1800);
        assert_eq!(Dur::parse("2h").unwrap().secs(), 7200);
        assert_eq!(Dur::parse("7d").unwrap().secs(), 604800);
        assert_eq!(Dur::parse("90s").unwrap().secs(), 90);
        assert!(Dur::parse("5x").is_err());
        assert!(Dur::parse("h").is_err());
        assert!(Dur::parse("5").is_err());
    }
}
